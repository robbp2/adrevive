from flask import Flask, request, render_template, send_file, jsonify, session, after_this_request
import os
from werkzeug.utils import secure_filename
from PIL import Image, ImageDraw
import cv2
import random
import subprocess
import numpy as np
import zipfile
import io
import uuid
import tempfile
import shutil  # For directory cleanup
import time
import logging
import threading
from threading import Lock
from logging.handlers import RotatingFileHandler
import redis
from pillow_heif import register_heif_opener

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

# Register HEIF opener
register_heif_opener()

# Flask app setup
app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'outputs'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
app.secret_key = 'f8cl2k98cj3i4fnckac3'

job_status = {}
job_folders = {}  # Tracks job_id -> session output folder mapping

# Set up Flask logger
if not app.debug:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

# Set FFmpeg path
FFMPEG_PATH = "/usr/bin/ffmpeg"  # Update this if FFmpeg is installed elsewhere

@app.route('/')
def index():
    return render_template('index.html')

@app.before_request
def assign_session_id():
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())

@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, post-check=0, pre-check=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.route('/clear', methods=['POST'])
def clear_files():

    # Get the session ID and session directories
    session_id = session.get('session_id')
    if not session_id:
        return jsonify({"error": "No active session"}), 400

    session_upload_folder = os.path.join(UPLOAD_FOLDER, session_id)
    session_output_folder = os.path.join(OUTPUT_FOLDER, session_id)

    # Attempt to delete the session directories
    try:
        if os.path.exists(session_upload_folder):
            shutil.rmtree(session_upload_folder)
        if os.path.exists(session_output_folder):
            shutil.rmtree(session_output_folder)
        return jsonify({"success": "Files cleared successfully"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to clear files: {e}"}), 500

@app.route('/upload', methods=['POST'])
def upload_files():
    # Generate unique job_id
    job_id = str(uuid.uuid4())
    app.logger.info(f"Generated job_id: {job_id}")

    # Retrieve the number of variations from the request
    try:
        variations = int(request.form.get('variations', 1))
    except ValueError:
        return jsonify({"error": "Invalid variations value"}), 400

    # Create session-specific temporary directories
    session_upload_folder = tempfile.mkdtemp(dir=UPLOAD_FOLDER)
    session_output_folder = tempfile.mkdtemp(dir=OUTPUT_FOLDER)

    # Save folder paths in Redis
    redis_client.hset(f"job:{job_id}", "upload_folder", session_upload_folder)
    redis_client.hset(f"job:{job_id}", "output_folder", session_output_folder)

    if 'files[]' not in request.files:
        return jsonify({"error": "No files uploaded"}), 400

    files = request.files.getlist('files[]')
    if not files:
        return jsonify({"error": "No selected files"}), 400

    session_id = session.get('session_id')
    if not session_id:
        return jsonify({"error": "No active session"}), 400

    try:
        # Store job data in Redis
        total_files = len(files) * variations
        redis_client.hset(f"job:{job_id}", "total_files", total_files)
        redis_client.hset(f"job:{job_id}", "completed_files", 0)
        redis_client.hset(f"job:{job_id}", "status", "pending")
        redis_client.hset(f"job:{job_id}", "progress", 0)
        redis_client.hset(f"job:{job_id}", "output_folder", session_output_folder)
        app.logger.info(f"[UPLOAD] Job {job_id} initialized with folders: {session_upload_folder}, {session_output_folder}")

        # Process each file in the request
        for file in files:
            filename = secure_filename(file.filename)
            file_path = os.path.join(session_upload_folder, filename)
            file.save(file_path)  # Save the file to session_upload_folder

            for i in range(variations):
                variation_output_path = os.path.join(
                    session_output_folder, f"processed_{i+1}_{filename}"
                )

                # Determine file type and process accordingly
                if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.tiff', '.heic', '.webp')):
                    app.logger.info(f"[UPLOAD] Queued image file: {filename}")
                    threading.Thread(
                        target=modify_image, args=(file_path, variation_output_path, job_id, i), daemon=True
                    ).start()
                elif filename.lower().endswith(('.mp4', '.avi', '.mov')):
                    app.logger.info(f"[UPLOAD] Queued video file: {filename}")
                    threading.Thread(
                        target=modify_video, args=(file_path, variation_output_path, job_id, i), daemon=True
                    ).start()
                else:
                    app.logger.warning(f"Unsupported file type skipped: {filename}")
                    continue  # Skip unsupported files

        app.logger.info(f"All files queued for processing under Job ID {job_id}")
        return jsonify({"job_id": job_id})

    except Exception as e:
        app.logger.error(f"File processing failed: {e}")
        return jsonify({"error": f"File processing failed: {e}"}), 500


@app.route('/progress/<job_id>', methods=['GET'])
def get_progress(job_id):

    job_data = redis_client.hgetall(f"job:{job_id}")

    if not job_data:
        app.logger.error(f"[PROGRESS] Job ID {job_id} not found.")
        return jsonify({"error": "Invalid job ID"}), 404

    return jsonify({
        "status": job_data.get("status"),
        "progress": int(job_data.get("progress", 0))
    })

@app.route('/download/<job_id>', methods=['GET'])
def download_files(job_id):
    job_data = redis_client.hgetall(f"job:{job_id}")
    if not job_data:
        app.logger.error(f"[DOWNLOAD] Job ID {job_id} not found.")
        return jsonify({"error": "Invalid job ID"}), 404

    session_output_folder = job_data.get("output_folder")
    session_upload_folder = job_data.get("upload_folder")

    if not session_output_folder or not os.path.exists(session_output_folder):
        app.logger.error(f"[DOWNLOAD] Output folder for Job ID {job_id} does not exist.")
        return jsonify({"error": "Output folder not found"}), 404

    processed_files = [
        os.path.join(session_output_folder, f)
        for f in os.listdir(session_output_folder)
        if f.startswith("processed_")
    ]
    if not processed_files:
        app.logger.error(f"[DOWNLOAD] No processed files found for Job ID {job_id}.")
        return jsonify({"error": "Processed files not found"}), 404

    zip_file_path = os.path.join(session_output_folder, f"processed_files_{job_id}.zip")
    if not os.path.exists(zip_file_path):
        with zipfile.ZipFile(zip_file_path, 'w') as zipf:
            for file in processed_files:
                zipf.write(file, os.path.basename(file))

    @after_this_request
    def remove_files(response):
        """Delete uploaded and output files after the ZIP file is sent."""
        try:
            # Remove output folder
            if session_output_folder and os.path.exists(session_output_folder):
                shutil.rmtree(session_output_folder)
                app.logger.info(f"Cleaned up output folder for job {job_id}")

            # Remove upload folder
            if session_upload_folder and os.path.exists(session_upload_folder):
                shutil.rmtree(session_upload_folder)
                app.logger.info(f"Cleaned up upload folder for job {job_id}")
        except Exception as e:
            app.logger.error(f"Failed to clean up files for job {job_id}: {e}")
        return response

    return send_file(zip_file_path, as_attachment=True)




def safe_remove(file_path):
    """Safely remove a file, ignoring errors if the file doesn't exist."""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        app.logger.error(f"Error removing file {file_path}: {e}")



# Function to add an invisible mesh overlay to images or frames
def add_invisible_mesh(image):
    if image.mode != "RGBA":
        image = image.convert("RGBA")
    
    width, height = image.size
    step = 10  # Mesh grid spacing

    # Create a transparent overlay
    overlay = Image.new("RGBA", (width, height), (255, 255, 255, 0))
    draw = ImageDraw.Draw(overlay)

    # Draw transparent points
    for x in range(0, width, step):
        for y in range(0, height, step):
            draw.point((x, y), fill=(0, 0, 0, 0))  # Fully transparent

    # Combine the overlay with the original image
    combined_image = Image.alpha_composite(image, overlay)
    return combined_image

def update_job_progress(job_id):
    completed_files = int(redis_client.hget(f"job:{job_id}", "completed_files") or 0)
    total_files = int(redis_client.hget(f"job:{job_id}", "total_files") or 1)
    progress = int((completed_files / total_files) * 100)

    redis_client.hset(f"job:{job_id}", "progress", progress)

    if completed_files >= total_files:
        redis_client.hset(f"job:{job_id}", "status", "completed")



# Function to modify images
def modify_image(input_path, output_path, job_id, variation):
    try:
        redis_client.hset(f"job:{job_id}", "status", "processing")
        image = Image.open(input_path)

        if image.mode not in ["RGB", "RGBA"]:
            image = image.convert("RGB")

        if image.mode != "RGBA":
            image = image.convert("RGB")

        # Apply a unique adjustment for each variation
        random.seed(variation)  # Ensure consistent randomness per variation
        adjustment = random.uniform(-0.1, 0.1)

        image_array = np.array(image, dtype=np.float32)
        image_array = np.clip(image_array * (1 + adjustment), 0, 255).astype(np.uint8)
        modified_image = Image.fromarray(image_array)

        # Add invisible mesh overlay
        modified_image = add_invisible_mesh(modified_image)

        # Save the modified image
        # Ensure PNG format is used for HEIC or unsupported formats
        if input_path.lower().endswith(".heic", ".webp"):
            output_path = os.path.splitext(output_path)[0] + ".png"

        # Save the modified image
        modified_image.save(output_path, format="PNG")

        redis_client.hincrby(f"job:{job_id}", "completed_files", 1)
        update_job_progress(job_id)

    except Exception as e:
        redis_client.hset(f"job:{job_id}", "status", f"error: {e}")


# Function to modify videos
def modify_video(input_path, output_path, job_id, variation):
    try:
        # Unique temporary paths for audio and video processing
        temp_audio_path = os.path.join(OUTPUT_FOLDER, f"{job_id}_{variation}_{os.path.basename(input_path)}_temp_audio.mp3")
        temp_video_path = os.path.join(OUTPUT_FOLDER, f"{job_id}_{variation}_{os.path.basename(input_path)}_temp_video.mp4")

        redis_client.hset(f"job:{job_id}", "status", "processing")

        # Extract audio
        audio_extraction_command = [
            FFMPEG_PATH, "-i", input_path, "-q:a", "0", "-map", "a", temp_audio_path
        ]
        subprocess.run(audio_extraction_command, check=True, capture_output=True, text=True, timeout=3000)

        # Process video frames
        cap = cv2.VideoCapture(input_path)
        if not cap.isOpened():
            raise Exception(f"Could not open video file: {input_path}")

        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

        # Adjust FPS randomly by ±5% for each variation
        original_fps = cap.get(cv2.CAP_PROP_FPS)
        random.seed(variation)  # Ensure consistent randomness per variation
        adjusted_fps = original_fps * random.uniform(0.95, 1.05)

        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(temp_video_path, fourcc, adjusted_fps, (width, height))

        if not out.isOpened():
            raise Exception(f"Failed to initialize VideoWriter for: {output_path}")

        # Uniform brightness adjustment
        adjustment = random.uniform(-0.05, 0.05)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        processed_frames = 0

        while True:
            ret, frame = cap.read()
            if not ret:
                break

            # Modify the frame
            frame = frame.astype(np.float32)
            frame = np.clip(frame * (1 + adjustment), 0, 255).astype(np.uint8)

            # Add invisible mesh overlay
            frame_image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
            frame_image = add_invisible_mesh(frame_image)

            # Convert back and write to output
            frame = cv2.cvtColor(np.array(frame_image), cv2.COLOR_RGB2BGR)
            out.write(frame)

            processed_frames += 1

            # Update progress periodically
            if total_frames > 0:
                progress = int((processed_frames / total_frames) * 100)
                redis_client.hset(f"job:{job_id}", "progress", progress)

        cap.release()
        out.release()

        # Combine audio and video with randomized speed adjustment
        speed_factor = random.uniform(0.95, 1.05)  # ±5% speed adjustment
        combine_command = [
            FFMPEG_PATH, "-i", temp_video_path, "-i", temp_audio_path,
            "-filter:v", f"setpts={1/speed_factor}*PTS", "-c:a", "aac", output_path
        ]
        subprocess.run(combine_command, check=True, capture_output=True, text=True, timeout=3000)

    except Exception as e:
        app.logger.error(f"Error in video processing for job {job_id}, variation {variation}: {e}")
    finally:
        # Cleanup and progress update
        safe_remove(temp_audio_path)
        safe_remove(temp_video_path)
        redis_client.hincrby(f"job:{job_id}", "completed_files", 1)
        update_job_progress(job_id)

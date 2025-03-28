from flask import Flask, request, render_template, send_file, jsonify, session, after_this_request
import os
import gc
import json
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
from concurrent.futures import ThreadPoolExecutor
import psutil

# Create a thread pool with size matching your CPU count
executor = ThreadPoolExecutor(
    max_workers=4,  # Ensure at least 2 workers for parallel processing
    thread_name_prefix="processor_"
)
def check_system_resources():
    """Check if system has enough resources to process more files"""
    memory = psutil.virtual_memory()
    cpu_percent = psutil.cpu_percent()
    
    # If memory usage is above 80% or CPU above 90%, delay processing
    return memory.percent < 80 and cpu_percent < 90

def get_ffmpeg_limits():
    """Calculate appropriate FFmpeg resource limits based on system state"""
    memory = psutil.virtual_memory()
    available_memory_mb = memory.available / (1024 * 1024)
    
    # Allow FFmpeg to use at most 50% of available memory
    memory_limit = int(available_memory_mb * 0.5)
    
    # Limit to 2 threads (half of your vCPUs)
    thread_limit = 2
    
    return memory_limit, thread_limit

# Connect to Redis
redis_pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)

def get_redis_client():
    return redis.StrictRedis(connection_pool=redis_pool)

# Default Redis client for backwards compatibility
redis_client = get_redis_client()

# Register HEIF opener
register_heif_opener()

# Flask app setup
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024 * 1024  # 1GB max-limit
app.config['MAX_CONTENT_PATH'] = None  # No path length limit
UPLOAD_FOLDER = '/mnt/ramdisk/uploads'
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

# ========== START NEW CODE FOR BATCH UPLOAD ==========

@app.route('/start-batch-job', methods=['POST'])
def start_batch_job():
    """
    Initialize a new batch upload job.
    This should be called before starting to upload individual files.
    """
    # Check system resources before accepting new uploads
    if not check_system_resources():
        return jsonify({"error": "Server is currently under heavy load. Please try again later."}), 503
    
    # Generate unique job_id
    job_id = str(uuid.uuid4())
    app.logger.info(f"Generated batch job_id: {job_id}")

    # Get file count and variations
    try:
        total_file_count = int(request.form.get('total_files', 0))
        variations = int(request.form.get('variations', 1))
        
        if total_file_count <= 0:
            return jsonify({"error": "Invalid file count"}), 400
    except ValueError:
        return jsonify({"error": "Invalid number format"}), 400

    # Create session-specific temporary directories
    session_upload_folder = tempfile.mkdtemp(dir=UPLOAD_FOLDER)
    session_output_folder = tempfile.mkdtemp(dir=OUTPUT_FOLDER)

    # Save folder paths and job info in Redis
    redis_client = get_redis_client()
    redis_client.hset(f"job:{job_id}", "upload_folder", session_upload_folder)
    redis_client.hset(f"job:{job_id}", "output_folder", session_output_folder)
    redis_client.hset(f"job:{job_id}", "total_files", total_file_count * variations)
    redis_client.hset(f"job:{job_id}", "completed_files", 0)
    redis_client.hset(f"job:{job_id}", "uploaded_files", 0)
    redis_client.hset(f"job:{job_id}", "status", "uploading")
    redis_client.hset(f"job:{job_id}", "progress", 0)
    redis_client.hset(f"job:{job_id}", "variations", variations)

    app.logger.info(f"[BATCH] Job {job_id} initialized for {total_file_count} files with {variations} variations")
    
    # Return job_id to client
    return jsonify({
        "job_id": job_id,
        "status": "initialized"
    })

@app.route('/upload-batch-file', methods=['POST'])
def upload_batch_file():
    """
    Upload a single file as part of a batch job.
    This endpoint should be called multiple times, once for each file.
    """
    job_id = request.form.get('job_id')
    if not job_id:
        return jsonify({"error": "Missing job_id parameter"}), 400
    
    redis_client = get_redis_client()
    job_data = redis_client.hgetall(f"job:{job_id}")
    
    if not job_data:
        return jsonify({"error": "Invalid job ID"}), 404
    
    session_upload_folder = job_data.get("upload_folder")
    
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Empty filename"}), 400
    
    try:
        # Save the file to the job's upload folder
        filename = secure_filename(file.filename)
        file_path = os.path.join(session_upload_folder, filename)
        file.save(file_path)
        
        # Update the counter for uploaded files
        redis_client.hincrby(f"job:{job_id}", "uploaded_files", 1)
        uploaded_files = int(redis_client.hget(f"job:{job_id}", "uploaded_files"))
        total_files = int(redis_client.hget(f"job:{job_id}", "total_files") or 0) / int(job_data.get("variations", 1))
        
        app.logger.info(f"[BATCH] File {uploaded_files}/{total_files} uploaded for job {job_id}: {filename}")
        
        return jsonify({
            "success": True,
            "job_id": job_id,
            "file": filename,
            "uploaded": uploaded_files,
            "total": total_files
        })
        
    except Exception as e:
        app.logger.error(f"Error uploading batch file: {e}")
        return jsonify({"error": f"Upload failed: {str(e)}"}), 500

@app.route('/process-batch-job', methods=['POST'])
def process_batch_job():
    """
    Start processing a batch job after all files have been uploaded.
    """
    job_id = request.form.get('job_id')
    if not job_id:
        return jsonify({"error": "Missing job_id parameter"}), 400
    
    redis_client = get_redis_client()
    job_data = redis_client.hgetall(f"job:{job_id}")
    
    if not job_data:
        return jsonify({"error": "Invalid job ID"}), 404
    
    session_upload_folder = job_data.get("upload_folder")
    session_output_folder = job_data.get("output_folder")
    variations = int(job_data.get("variations", 1))
    
    try:
        # Get list of all uploaded files
        uploaded_files = os.listdir(session_upload_folder)
        if not uploaded_files:
            return jsonify({"error": "No files found for processing"}), 400
        
        # Update job status to processing
        redis_client.hset(f"job:{job_id}", "status", "processing")
        redis_client.hset(f"job:{job_id}", "progress", 0)
        
        # Create task list for processing
        processing_tasks = []
        
        for filename in uploaded_files:
            file_path = os.path.join(session_upload_folder, filename)
            
            for i in range(variations):
                variation_output_path = os.path.join(
                    session_output_folder, f"processed_{i+1}_{filename}"
                )
                
                # Add to the task list
                if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.tiff', '.heic', '.webp')):
                    app.logger.info(f"[BATCH] Queued image file: {filename}")
                    processing_tasks.append({
                        'type': 'image',
                        'path': file_path,
                        'output': variation_output_path,
                        'job_id': job_id,
                        'variation': i
                    })
                elif filename.lower().endswith(('.mp4', '.avi', '.mov')):
                    app.logger.info(f"[BATCH] Queued video file: {filename}")
                    processing_tasks.append({
                        'type': 'video',
                        'path': file_path,
                        'output': variation_output_path,
                        'job_id': job_id,
                        'variation': i
                    })
                else:
                    app.logger.warning(f"Unsupported file type skipped: {filename}")
                    continue
        
        # Launch a separate thread to process all files
        def process_all_files():
            try:
                for task in processing_tasks:
                    if task['type'] == 'image':
                        modify_image(task['path'], task['output'], task['job_id'], task['variation'])
                    else:  # video
                        modify_video(task['path'], task['output'], task['job_id'], task['variation'])
                app.logger.info(f"All files processed for Batch Job ID {job_id}")
            except Exception as e:
                app.logger.error(f"Error processing files for batch job {job_id}: {e}")
                
        # Start a background thread for processing
        threading.Thread(target=process_all_files, daemon=True).start()
        
        app.logger.info(f"All files queued for processing under Batch Job ID {job_id}")
        return jsonify({
            "success": True,
            "job_id": job_id,
            "files_to_process": len(processing_tasks)
        })
        
    except Exception as e:
        app.logger.error(f"Failed to start batch processing: {e}")
        return jsonify({"error": f"Processing failed: {str(e)}"}), 500

# ========== END NEW CODE FOR BATCH UPLOAD ==========

# Keep the original upload endpoint for backward compatibility
@app.route('/upload', methods=['POST'])
def upload_files():
    # Check system resources before accepting new uploads
    if not check_system_resources():
        return jsonify({"error": "Server is currently under heavy load. Please try again later."}), 503
    
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
    redis_client = get_redis_client()
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

        # Create a list of tasks
        processing_tasks = []
        
        for file in files:
            filename = secure_filename(file.filename)
            file_path = os.path.join(session_upload_folder, filename)
            file.save(file_path)  # Save the file to session_upload_folder

            for i in range(variations):
                variation_output_path = os.path.join(
                    session_output_folder, f"processed_{i+1}_{filename}"
                )
                
                # Add to the task list
                if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.tiff', '.heic', '.webp')):
                    app.logger.info(f"[UPLOAD] Queued image file: {filename}")
                    processing_tasks.append({
                        'type': 'image',
                        'path': file_path,
                        'output': variation_output_path,
                        'job_id': job_id,
                        'variation': i
                    })
                elif filename.lower().endswith(('.mp4', '.avi', '.mov')):
                    app.logger.info(f"[UPLOAD] Queued video file: {filename}")
                    processing_tasks.append({
                        'type': 'video',
                        'path': file_path,
                        'output': variation_output_path,
                        'job_id': job_id,
                        'variation': i
                    })
                else:
                    app.logger.warning(f"Unsupported file type skipped: {filename}")
                    continue  # Skip unsupported files

        # Launch a separate thread to process all files
        def process_all_files():
            try:
                for task in processing_tasks:
                    if task['type'] == 'image':
                        modify_image(task['path'], task['output'], task['job_id'], task['variation'])
                    else:  # video
                        modify_video(task['path'], task['output'], task['job_id'], task['variation'])
                app.logger.info(f"All files processed for Job ID {job_id}")
            except Exception as e:
                app.logger.error(f"Error processing files for job {job_id}: {e}")
                
        # Start a background thread for processing
        threading.Thread(target=process_all_files, daemon=True).start()
        
        app.logger.info(f"All files queued for processing under Job ID {job_id}")
        return jsonify({"job_id": job_id})

    except Exception as e:
        app.logger.error(f"File processing failed: {e}")
        return jsonify({"error": f"File processing failed: {e}"}), 500


@app.route('/progress/<job_id>', methods=['GET'])
def get_progress(job_id):
    redis_client = get_redis_client()
    job_data = redis_client.hgetall(f"job:{job_id}")

    if not job_data:
        app.logger.error(f"[PROGRESS] Job ID {job_id} not found.")
        return jsonify({"error": "Invalid job ID"}), 404

    status = job_data.get("status")
    step = job_data.get("step", "")
    
    # Adjust the progress weighting - uploading is now just 5% of total progress
    if status == "uploading":
        # During upload phase (5% of total progress)
        uploaded = int(job_data.get("uploaded_files", 0))
        total = int(job_data.get("total_files", 0)) / int(job_data.get("variations", 1))
        if total > 0:
            upload_progress = (uploaded / total) * 5  # Upload is 5% of total progress
            overall_progress = int(upload_progress)
        else:
            overall_progress = 0
    elif status == "processing":
        # During processing phase (95% of total progress)
        completed = int(job_data.get("completed_files", 0))
        total = int(job_data.get("total_files", 0))
        if total > 0:
            process_progress = (completed / total) * 95  # Processing is 95% of total progress
            overall_progress = 5 + int(process_progress)  # Add 5% from upload phase
        else:
            overall_progress = 5  # Default to upload phase completed
    elif status == "completed":
        overall_progress = 100
    else:
        overall_progress = int(job_data.get("progress", 0))

    return jsonify({
        "status": status,
        "progress": overall_progress,
        "step": step,
        "phase": "uploading" if status == "uploading" else "processing"
    })

@app.route('/download/<job_id>', methods=['GET'])
def download_files(job_id):
    redis_client = get_redis_client()
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
    redis_client = get_redis_client()
    completed_files = int(redis_client.hget(f"job:{job_id}", "completed_files") or 0)
    total_files = int(redis_client.hget(f"job:{job_id}", "total_files") or 1)
    progress = int((completed_files / total_files) * 100)

    redis_client.hset(f"job:{job_id}", "progress", progress)

    if completed_files >= total_files:
        redis_client.hset(f"job:{job_id}", "status", "completed")


# Function to modify images
def modify_image(input_path, output_path, job_id, variation):
    redis_client = get_redis_client()
    # Set a timeout for image processing
    start_time = time.time()
    max_processing_time = 300  # 5 minutes max per image
    
    try:
        # Update job status to "processing"
        redis_client.hset(f"job:{job_id}", "status", "processing")

        # Check if file exists and is not too large
        file_size = os.path.getsize(input_path)
        if file_size > 100 * 1024 * 1024:  # 100MB
            raise ValueError(f"Image file too large: {file_size/1024/1024:.2f}MB")

        # Open the image file
        image = Image.open(input_path)

        # Ensure the image is in the correct mode
        if image.mode not in ["RGB", "RGBA"]:
            image = image.convert("RGB")

        if image.mode != "RGBA":
            image = image.convert("RGB")

        # Apply a unique adjustment for each variation
        random.seed(variation)  # Ensure consistent randomness per variation
        adjustment = random.uniform(-0.1, 0.1)

        # Adjust image pixels
        image_array = np.array(image, dtype=np.float32)
        image_array = np.clip(image_array * (1 + adjustment), 0, 255).astype(np.uint8)
        modified_image = Image.fromarray(image_array)

        # Add invisible mesh overlay
        modified_image = add_invisible_mesh(modified_image)

        # Save the modified image
        # Ensure PNG format is used for HEIC or unsupported formats
        if input_path.lower().endswith((".heic", ".webp")):  # Fixed tuple syntax for extensions
            output_path = os.path.splitext(output_path)[0] + ".png"

        modified_image.save(output_path, format="PNG")

        # Increment the completed files count in Redis
        redis_client.hincrby(f"job:{job_id}", "completed_files", 1)

        # Update the job's progress
        update_job_progress(job_id)

    except TimeoutError as e:
        app.logger.error(f"Timeout error processing image for job {job_id}, variation {variation}: {e}")
        redis_client.hset(f"job:{job_id}", "status", f"error: Processing timeout - image too large")
    except Exception as e:
        # Set job status to error with the exception message
        redis_client.hset(f"job:{job_id}", "status", f"error: {str(e)[:100]}")
        app.logger.error(f"Error in image processing for job {job_id}, variation {variation}: {e}")
    finally:
        # Force garbage collection
        gc.collect()


# Function to modify videos
def modify_video(input_path, output_path, job_id, variation):
    # Set execution limits
    start_time = time.time()
    max_processing_time = 1800  # 30 minutes max per video
    
    # Create unique temporary directory for this job
    temp_dir = os.path.join(OUTPUT_FOLDER, f"temp_{job_id}_{variation}")
    os.makedirs(temp_dir, exist_ok=True)
    
    # Temporary file paths
    temp_audio_path = os.path.join(temp_dir, "temp_audio.mp3")
    temp_video_path = os.path.join(temp_dir, "temp_video.mp4")
    
    try:
        redis_client = get_redis_client()
        redis_client.hset(f"job:{job_id}", "status", "processing")
        
        # Basic file checks
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file does not exist: {input_path}")
            
        file_size = os.path.getsize(input_path)
        if file_size == 0:
            raise ValueError(f"Input file is empty: {input_path}")
            
        # Set randomization seed for consistent results per variation
        random.seed(variation)
        adjustment = random.uniform(-0.05, 0.05)  # Brightness adjustment
        speed_factor = random.uniform(0.95, 1.05)  # Speed adjustment
        
        # Get an estimate of the total work: audio extraction + frame processing + final encoding
        # Audio extraction is ~10% of work
        # Frame processing is ~80% of work  
        # Final encoding is ~10% of work
        
        # Step 1: Extract audio (~10% of work)
        redis_client.hset(f"job:{job_id}", "step", "extracting_audio")
        app.logger.info(f"Extracting audio for {os.path.basename(input_path)}")
        audio_extraction_command = [
            FFMPEG_PATH, "-i", input_path, 
            "-q:a", "0", "-map", "a", 
            temp_audio_path
        ]
        subprocess.run(audio_extraction_command, check=True, capture_output=True, text=True, timeout=600)
        
        # Step 2: Process video frames (~80% of work)
        redis_client.hset(f"job:{job_id}", "step", "processing_frames")
        cap = cv2.VideoCapture(input_path)
        if not cap.isOpened():
            raise Exception(f"Could not open video file: {input_path}")
        
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        
        # Adjust FPS randomly by ±5% for each variation
        original_fps = cap.get(cv2.CAP_PROP_FPS)
        adjusted_fps = original_fps * random.uniform(0.95, 1.05)
        
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(temp_video_path, fourcc, adjusted_fps, (width, height))
        
        if not out.isOpened():
            raise Exception(f"Failed to initialize VideoWriter for: {output_path}")
        
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        processed_frames = 0
        
        # Process in batches to manage memory better
        batch_size = 100  # Process frames in batches
        
        # Get the current file's weight of the total job
        job_total_files = int(redis_client.hget(f"job:{job_id}", "total_files") or 1)
        completed_files = int(redis_client.hget(f"job:{job_id}", "completed_files") or 0)
        
        while True:
            batch_frames = 0
            
            # Process a batch of frames
            while batch_frames < batch_size:
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
                batch_frames += 1
                
                # Calculate DETAILED progress including: 
                # - Which file in the sequence we're processing
                # - How far we are in processing this specific file
                # - Account for the 3-part process (10% audio, 80% frames, 10% encoding)
                if total_frames > 0 and processed_frames % 10 == 0:
                    # Calculate frame processing progress for current file (0-80%)
                    frame_progress = (processed_frames / total_frames) * 80
                    # Add 10% for audio extraction which is already done
                    file_progress = 10 + frame_progress
                    
                    # Now calculate overall job progress
                    file_weight = 1 / job_total_files
                    overall_progress = (completed_files * file_weight * 100) + (file_progress * file_weight)
                    # Round to nearest integer and set
                    redis_client.hset(f"job:{job_id}", "progress", int(overall_progress))
            
            # Check if we're done
            if batch_frames < batch_size:
                break
            
            # Force garbage collection after batch
            gc.collect()
            
            # Check timeout
            if time.time() - start_time > max_processing_time:
                raise TimeoutError(f"Frame processing exceeded time limit for {os.path.basename(input_path)}")
        
        cap.release()
        out.release()
        
        # Step 3: Combine audio and video with speed adjustment (~10% of work)
        redis_client.hset(f"job:{job_id}", "step", "encoding_final")
        app.logger.info(f"Combining audio and video for {os.path.basename(input_path)}")
        combine_command = [
            FFMPEG_PATH, "-i", temp_video_path, "-i", temp_audio_path,
            "-filter:v", f"setpts={1/speed_factor}*PTS", 
            "-c:a", "aac", output_path
        ]
        subprocess.run(combine_command, check=True, capture_output=True, text=True, timeout=900)
        
        app.logger.info(f"Successfully processed video: {os.path.basename(input_path)}")
        
    except TimeoutError as e:
        app.logger.error(f"Timeout error processing video for job {job_id}, variation {variation}: {e}")
        redis_client = get_redis_client()
        redis_client.hset(f"job:{job_id}", "status", f"error: Processing timeout - video too large")
    except Exception as e:
        app.logger.error(f"Error in video processing for job {job_id}, variation {variation}: {e}")
        redis_client = get_redis_client()
        redis_client.hset(f"job:{job_id}", "status", f"error: {str(e)[:100]}")
    finally:
        # Cleanup temporary files and directory
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as e:
            app.logger.error(f"Error cleaning up temp files: {e}")
        
        # Update progress
        redis_client = get_redis_client()
        redis_client.hincrby(f"job:{job_id}", "completed_files", 1)
        update_job_progress(job_id)
        
        # Force garbage collection
        gc.collect()

# Helper function to process a video segment
def process_video_segment(segment_path, output_path, adjustment, speed_factor=1.0):
    """Process a single video segment with the given adjustment"""
    cap = cv2.VideoCapture(segment_path)
    if not cap.isOpened():
        raise Exception(f"Could not open video segment: {segment_path}")
    
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
    
    if not out.isOpened():
        raise Exception(f"Failed to initialize VideoWriter for segment: {output_path}")
    
    # Process in batches
    batch_size = 50
    batch_count = 0
    
    while True:
        frame_count = 0
        while frame_count < batch_size:
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
            
            frame_count += 1
            
        # If we didn't process a full batch, we're done
        if frame_count < batch_size:
            break
            
        # Force garbage collection after each batch
        batch_count += 1
        if batch_count % 2 == 0:  # Every other batch
            gc.collect()
    
    cap.release()
    out.release()

if __name__ == "__main__":
    # Configure gunicorn with appropriate timeouts
    options = {
        'bind': '0.0.0.0:8000',
        'workers': 4,
        'timeout': 300,  # Increase timeout to 5 minutes
        'keepalive': 5
    }
    
    # Normally would use the StandaloneApplication class here
    # but we'll let gunicorn_config.py handle that
    app.run(debug=False)
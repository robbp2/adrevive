import os
import time
import shutil

UPLOAD_FOLDER = '/path/to/uploads'  # Replace with your actual upload folder
OUTPUT_FOLDER = '/path/to/outputs'  # Replace with your actual output folder
CUTOFF_TIME = 60 * 60  # 1 hour in seconds

def cleanup_folder(folder_path):
    now = time.time()
    for root, dirs, files in os.walk(folder_path):
        for directory in dirs:
            dir_path = os.path.join(root, directory)
            if os.path.isdir(dir_path):
                # Check if the directory is older than the cutoff time
                if now - os.path.getmtime(dir_path) > CUTOFF_TIME:
                    shutil.rmtree(dir_path, ignore_errors=True)
                    print(f"Deleted: {dir_path}")

if __name__ == "__main__":
    print("Starting cleanup...")
    cleanup_folder(UPLOAD_FOLDER)
    cleanup_folder(OUTPUT_FOLDER)
    print("Cleanup completed.")

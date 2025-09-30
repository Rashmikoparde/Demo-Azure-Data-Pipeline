# run_simple.py
import os
import time
import uvicorn
from watchdog.observers import Observer
from src.triggers.folder_watcher import NewFileHandler
from src.local_web_app.upload_app import app

def run_simple_pipeline():
    """Simple sequential pipeline runner with both components."""
    print("🚀 Starting Simple Sensor Data Pipeline")
    print("=" * 50)
    
    # Setup
    directories = ["./data/landing", "./data/raw"]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✅ Created: {directory}")
    
    # Start folder watcher
    print("👀 Starting folder watcher...")
    watch_path = "./data/landing"
    event_handler = NewFileHandler()
    observer = Observer()
    observer.schedule(event_handler, watch_path, recursive=False)
    observer.start()
    print(f"✅ Folder watcher started on: {os.path.abspath(watch_path)}")
    
    print("\n🔧 Starting upload app...")
    print(" Upload app: http://127.0.0.1:8000")
    print(" Folder watcher: Active on ./data/landing/")
    print("\nPress Ctrl+C to stop")
    
    try:
        # Run upload app (this blocks - keeps the watcher running)
        uvicorn.run(app, host="127.0.0.1", port=8000)
    except KeyboardInterrupt:
        print("\n Stopping pipeline...")
    finally:
        # Clean up folder watcher
        observer.stop()
        observer.join()
        print("✅ Folder watcher stopped")
        print("🎯 Pipeline shutdown complete")

if __name__ == "__main__":
    run_simple_pipeline()
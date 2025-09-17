# src/triggers/folder_watcher.py
import sys
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Add the project root to Python path
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.append(project_root)

# Now import your modules
from src.prefect_flows.flows.data_ingestion_flow import data_ingestion_flow

class NewFileHandler(FileSystemEventHandler):
    """Handler for new file events in the raw data directory."""
    
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            print(f"\nNew file detected: {event.src_path}")
            print("Starting data ingestion flow...")
            
            # Run the Prefect flow
            result = data_ingestion_flow(event.src_path)
            
            if result['status'] == 'success':
                print(f"Processing completed: {result['output_path']}")
            else:
                print(f"Processing failed: {result['error']}")

def start_folder_watcher(watch_path: str = "./data/raw"):
    """Start watching a folder for new files."""
    # Create the directory if it doesn't exist
    os.makedirs(watch_path, exist_ok=True)
    
    print(f"Watching folder for new CSV files: {os.path.abspath(watch_path)}")
    print("Drop CSV files in this folder to trigger processing...")
    print("Press Ctrl+C to stop watching")
    
    event_handler = NewFileHandler()
    observer = Observer()
    observer.schedule(event_handler, watch_path, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nStopping folder watcher...")
    
    observer.join()

if __name__ == "__main__":
    start_folder_watcher()
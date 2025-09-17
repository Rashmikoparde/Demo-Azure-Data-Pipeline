import sys
import os

# Add the current directory to Python path
sys.path.append(os.path.dirname(__file__))

def run_folder_watcher():
    """Run the folder watcher."""
    from src.triggers.folder_watcher import start_folder_watcher
    start_folder_watcher()

def run_web_app():
    """Run the web upload app."""
    from src.local_web_app.upload_app import app
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    print("Choose an option:")
    print("1. Run Folder Watcher (simulates Azure Function)")
    print("2. Run Web Upload App")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        run_folder_watcher()
    elif choice == "2":
        run_web_app()
    else:
        print("Invalid choice. Please run again.")
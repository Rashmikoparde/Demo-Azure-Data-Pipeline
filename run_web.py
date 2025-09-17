# run_web.py
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(__file__))

try:
    from src.local_web_app.upload_app import run_server
    print("Starting web server...")
    run_server()
except ImportError as e:
    print(f"Import error: {e}")
    print("Trying alternative approach...")
    
    # Alternative: run directly
    import uvicorn
    uvicorn.run("src.local_web_app.upload_app:app", host="127.0.0.1", port=8000, reload=True)
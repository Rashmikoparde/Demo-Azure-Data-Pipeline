import sys
import os
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import shutil
import uvicorn

# Add the project root to Python path
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
if project_root not in sys.path:
    sys.path.append(project_root)

#try:
 #   from src.prefect_flows.flows.data_ingestion_flow import data_ingestion_flow
 #   print("Successfully imported data_ingestion_flow")
#except ImportError as e:
 #   print(f"Import error: {e}")
    # Create a mock function for testing
  #  def data_ingestion_flow(file_path):
   #     return {"status": "success", "output_path": f"processed_{file_path}"}

app = FastAPI(title="Sensor Data Upload API", debug=True)

# Ensure raw directory exists
os.makedirs("./data/raw", exist_ok=True)

@app.get("/", response_class=HTMLResponse)
async def upload_form():
    """Simple HTML form for file upload."""
    return """
    <html>
        <head>
            <title>Sensor Data Upload</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                form { border: 2px solid #007bff; padding: 20px; border-radius: 10px; }
                input[type="file"] { margin: 10px 0; }
                input[type="submit"] { 
                    background-color: #007bff; color: white; padding: 10px 20px;
                    border: none; border-radius: 5px; cursor: pointer;
                }
                input[type="submit"]:hover { background-color: #0056b3; }
            </style>
        </head>
        <body>
            <h2>📤 Upload Sensor Data CSV</h2>
            <form action="/upload" method="post" enctype="multipart/form-data">
                <input type="file" name="file" accept=".csv" required>
                <br>
                <input type="submit" value="Upload and Process">
            </form>
            <p>📁 Files will be processed and saved in the cleansed directory</p>
        </body>
    </html>
    """

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Handle file upload and trigger processing."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    try:
        # Save file to raw directory
        file_location = f"./data/landing/{file.filename}"
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        print(f"File saved: {file_location}")
        
        # Trigger processing
        #print("Starting data processing...")
        #result = data_ingestion_flow(file_location)
        
        response_data = {
            "filename": file.filename,
            "saved_location": file_location,
            "message": "File uploaded successfully to landing zone. Watchdog will trigger processing automatically."
        }
        
        return JSONResponse(content=response_data)
        
    except Exception as e:
        error_msg = f"Error processing file: {str(e)}"
        print(f"{error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "message": "Server is running"}

def run_server(host: str = "127.0.0.1", port: int = 8000):
    """Run the web server."""
    print(f"Starting web server at http://{host}:{port}")
    print("Open http://127.0.0.1:8000 in your browser")
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    run_server(host="127.0.0.1", port=8000)
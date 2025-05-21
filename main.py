from fastapi import FastAPI, UploadFile, File, Form, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import tempfile
import shutil
import os
from parse_file import parse_results

app = FastAPI()

# CORS setup for frontend to call this from anywhere
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# POST endpoint to handle main functionality
@app.post("/")
async def parse_track_results(
    file: UploadFile = File(...),
    meetDate: str = Form(...),
    edition: str = Form(...),
    meetName: str = Form(...),
    meetLocation: str = Form(...),
    season: str = Form(...),
    url: str = Form(...),
    timing: str = Form(...),
):
    try:
        # Save uploaded file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name

        # Build metadata dictionary
        metadata = {
            "Meet Date": meetDate,
            "Edition": edition,
            "Meet Name": meetName,
            "Meet Location": meetLocation,
            "Season": season,
            "URL": url,
            "Timing": timing
        }

        # Call parser and get the output file path
        output_file = parse_results(tmp_path, metadata)

        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

        # Read the output CSV file
        with open(output_file, "rb") as csv_file:
            csv_content = csv_file.read()
        
        # Clean up the output file
        if os.path.exists(output_file):
            os.unlink(output_file)

        # Return the CSV file for download
        filename = f"{meetName.replace(' ', '_')}_results.csv"
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    
    except Exception as e:
        return {"success": False, "error": str(e)}
    
# Simple GET endpoint for testing
@app.get("/")
async def health_check():
    return {"status": "API is running"}
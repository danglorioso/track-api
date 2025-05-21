from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import tempfile
import shutil
from .parse_file import parse_results

app = FastAPI()

# Optional CORS setup for frontend to call this from anywhere
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change this to ["http://localhost:3000"] in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/parse-results")
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

        # Call your existing parser
        parse_results(tmp_path, metadata)

        return {"success": True, "message": "File parsed successfully"}
    
    except Exception as e:
        return {"success": False, "error": str(e)}

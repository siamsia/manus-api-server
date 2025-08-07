import os
import json
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials
import gspread
import logging
logging.basicConfig(level=logging.INFO)

app = FastAPI()

# ====== SETUP GOOGLE DRIVE API ======
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
service_account_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])
creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
gc = gspread.authorize(creds)

drive_service = build('drive', 'v3', credentials=creds)
FOLDER_ID = '1fecni5SG7jN97nlWpYePpRaF3XgES8f2'  # <-- Folder ID ของ Google Drive

SHEET_ID = "1bwjLe1Q92SP4OFqrfsqrOnn9eAtTKKCXFIpwVT2oB50"  # ใส่ Spreadsheet ID
LOG_SHEET = "logs"
PROMPT_SHEET = "prompts"

# ====== Models ======
class ImageLog(BaseModel):
    id: str
    filename: str
    title: str
    keywords: List[str]
    prompt: str

# ====== MODEL สำหรับ prompt ======
class MarkPromptRequest(BaseModel):
    topic: str
    status: str = "done"


# ====== API: Load todo.txt / image_history.txt ======
@app.get("/load/{filename}")
async def load_file(filename: str):
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            content = file.read()
        return {"status": "success", "content": content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ====== API: Save todo.txt / image_history.txt ======
@app.post("/save/{filename}")
async def save_file(filename: str, request: Request):
    try:
        data = await request.json()
        content = data.get('content', '')
        with open(filename, 'w', encoding='utf-8') as file:
            file.write(content)
        return {"status": "success", "message": f"{filename} saved successfully!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ====== API: Upload ZIP file to local folder ======
@app.post("/upload/zip")
async def upload_zip(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="No selected file")
        os.makedirs('uploads', exist_ok=True)
        save_path = os.path.join('uploads', file.filename)
        with open(save_path, 'wb') as f:
            f.write(await file.read())
        return {"status": "success", "filename": file.filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ====== API: List uploaded files ======
@app.get("/list_uploads")
async def list_uploads():
    try:
        files = os.listdir('uploads')
        return files
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ====== API: Download ZIP from uploads ======
@app.get("/download/zip/{filename}")
async def download_zip(filename: str):
    file_path = os.path.join('uploads', filename)
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=filename, media_type='application/zip')
    else:
        raise HTTPException(status_code=404, detail="File not found")


# ====== API: Upload logs 1 row per image ======
@app.post("/upload/log")
async def upload_logs(logs: List[ImageLog]):
    try:
        sheet = client.open_by_key(SHEET_ID).worksheet(LOG_SHEET_NAME)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for log in logs:
            keywords = (log.keywords + [''] * 5)[:5]  # เติมให้ครบ 5 คำ
            row = [
                log.id,
                timestamp,
                log.filename,
                log.title,
                *keywords,
                log.prompt
            ]
            rows.append(row)
        sheet.append_rows(rows)
        return {"status": "success", "rows_uploaded": len(rows)}
    except Exception as e:
        logging.exception("Error while uploading logs")
        raise HTTPException(status_code=500, detail=str(e))

# ====== API: Get next unused prompt ======
@app.get("/prompt/next")
async def get_next_prompt():
    try:
        sheet = client.open_by_key(SHEET_ID).worksheet(PROMPT_SHEET_NAME)
        data = sheet.get_all_records()
        for i, row in enumerate(data, start=2):  # เริ่มแถว 2 เพราะแถว 1 header
            if not row.get("status"):
                topic = row.get("topic")
                prompts = json.loads(row.get("prompts", "[]"))
                return {"topic": topic, "prompts": prompts, "row": i}
        raise HTTPException(status_code=404, detail="No available prompt")
    except Exception as e:
        logging.exception("Error while prompt/next")
        raise HTTPException(status_code=500, detail=str(e))


# ====== API: Mark prompt as used ======
@app.post("/prompt/mark")
async def mark_prompt_used(req: MarkPromptRequest):
    try:
        sheet = client.open_by_key(SHEET_ID).worksheet(PROMPT_SHEET_NAME)
        data = sheet.get_all_records()
        for i, row in enumerate(data, start=2):
            if row.get("topic") == req.topic:
                sheet.update_cell(i, 3, req.status)  # สมมุติ column C เป็น status
                return {"status": "marked", "row": i}
        raise HTTPException(status_code=404, detail="Topic not found")
    except Exception as e:
        logging.exception("Error while prompt/mark")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    from os import environ
    app.run(host='0.0.0.0', port=int(environ.get('PORT', 3000)))





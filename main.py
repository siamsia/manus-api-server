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

PROMPT_SHEET_NAME = "prompts"
LOG_SHEET_NAME = "logs"

sh = gc.open_by_key(SHEET_ID)
promptsheet = sh.worksheet(PROMPT_SHEET_NAME)
logsheet = sh.worksheet(LOG_SHEET_NAME)

# ====== Models ======
class ImageLog(BaseModel):
    id: str
    filename: str
    title: str
    keywords: List[str]
    prompt: str

# === Schema ===
class MarkPromptRequest(BaseModel):
    topicId: int


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
        logsheet.append_rows(rows)
        return {"status": "success", "rows_uploaded": len(rows)}
    except Exception as e:
        logging.exception("Error while uploading logs")
        raise HTTPException(status_code=500, detail=str(e))

# === API: ดึง topic ถัดไปที่ยังไม่ mark ===
@app.get("/get_next_prompt")
async def get_next_prompt():
    try:
        data = promptsheet.get_all_records()
        for idx, row in enumerate(data):
            if not row.get("used", "").strip():  # ถ้ายังไม่ mark
                topic = row.get("topic", "")
                prompts_raw = row.get("prompts", "")
                prompts = [p.strip() for p in prompts_raw.split("\n") if p.strip()]
                return {
                    "topicId": idx + 2,  # แถวใน Google Sheets (เริ่มจาก 2)
                    "topic": topic,
                    "prompts": prompts
                }
        return {"message": "No available prompts"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === API: mark prompt ว่าใช้แล้ว ===
@app.post("/mark_prompt_used")
async def mark_prompt_used(req: MarkPromptRequest):
    try:
        row_number = req.topicId
        if row_number < 2:
            raise HTTPException(status_code=400, detail="Invalid topicId")
        promptsheet.update_cell(row_number, 3, "yes")  # สมมติว่า 'used' อยู่คอลัมน์ C (column 3)
        return {"status": "success", "message": f"Marked row {row_number} as used"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    from os import environ
    app.run(host='0.0.0.0', port=int(environ.get('PORT', 3000)))







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
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_INFO = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])
creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
sheet_service = build('sheets', 'v4', credentials=creds)
SPREADSHEET_ID = '1bwjLe1Q92SP4OFqrfsqrOnn9eAtTKKCXFIpwVT2oB50'  # 👉 เปลี่ยนเป็นของจริง
SHEET_NAME = 'prompts'  # 👉 หรือชื่อ sheet ที่เซี้ยตั้งไว้

# ====== Models ======
class ImageLog(BaseModel):
    id: str
    filename: str
    title: str
    keywords: List[str]
    prompt: str

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

@app.get("/get_next_prompt")
async def get_next_prompt():
    try:
        sheet = sheet_service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=SHEET_NAME
        ).execute()
        values = result.get("values", [])

        if not values or len(values) < 2:
            return {"prompts": []}

        headers = values[0]
        rows = values[1:]

        # หาตำแหน่งของแต่ละ column
        idx_map = {h: i for i, h in enumerate(headers)}
        required_cols = ["rowId", "topic", "prompt", "used"]
        if not all(col in idx_map for col in required_cols):
            raise HTTPException(status_code=500, detail="Missing required columns")

        # กรองเฉพาะแถวที่ยังไม่ถูกใช้ (ปลอดภัยต่อ row ที่มีคอลัมน์ไม่ครบ)
        unused = []
        for row in rows:
            used_val = row[idx_map["used"]] if len(row) > idx_map["used"] else ""
            if used_val.strip() == "":
                unused.append(row)

        if not unused:
            return {"prompts": []}

        # แยกตาม topic แล้วเลือกเฉพาะ topic แรก
        grouped = defaultdict(list)
        for row in unused:
            topic = row[idx_map["topic"]]
            grouped[topic].append(row)

        first_topic = sorted(grouped.keys())[0]
        selected_rows = grouped[first_topic]

        # คืนค่าเฉพาะข้อมูลที่ต้องการ
        output = []
        for row in selected_rows:
            output.append({
                "rowId": int(row[idx_map["rowId"]]),
                "topic": row[idx_map["topic"]],
                "prompt": row[idx_map["prompt"]],
            })

        return {"prompts": output}

    except Exception as e:
        logging.exception("Error while get_next_prompt")
        raise HTTPException(status_code=500, detail=str(e))


# ====== /mark_prompt_used ======
class MarkPromptRequest(BaseModel):
    rowIds: List[int]
    log_id: str

@app.post("/mark_prompt_used")
async def mark_prompt_used(request: MarkPromptRequest):
    try:
        result = sheet_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A2:L"
        ).execute()

        rows = result.get('values', [])
        now_thai = datetime.now(pytz.timezone("Asia/Bangkok")).strftime('%Y-%m-%d %H:%M:%S')

        updates = []
        for i, row in enumerate(rows):
            try:
                row_id = int(row[0])
            except (IndexError, ValueError):
                continue

            if row_id in request.rowIds:
                row_idx = i + 1 + 1  # บวก 1 เพราะเริ่ม A2 และอีก 1 เพราะ header
                updates.append({
                    "range": f"{SHEET_NAME}!J{row_idx}:L{row_idx}",
                    "values": [["yes", request.log_id, now_thai]]
                })

        if updates:
            body = {"valueInputOption": "RAW", "data": updates}
            sheet_service.spreadsheets().values().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=body
            ).execute()

        return {"status": "success", "marked": len(updates)}

    except Exception as e:
        logging.exception("Error while mark_prompt_used")
        raise HTTPException(status_code=500, detail=str(e))
        

if __name__ == '__main__':
    from os import environ
    app.run(host='0.0.0.0', port=int(environ.get('PORT', 3000)))













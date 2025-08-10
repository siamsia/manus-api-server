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
from collections import defaultdict
import pytz
from fastapi.middleware.cors import CORSMiddleware  # ⬅️ เพิ่มบรรทัดนี้

logging.basicConfig(level=logging.INFO)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # ถ้าจะจำกัดโดเมน ให้ใส่เป็นลิสต์ของโดเมนแทน "*"
    allow_credentials=True,
    allow_methods=["*"],          # หรือกำหนดเฉพาะ ["GET","POST","OPTIONS"]
    allow_headers=["*"],          # หรือกำหนดเฉพาะ headers ที่จะใช้
)

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

# === Models สำหรับ insert_prompts ===
class InsertRow(BaseModel):
    topic: str
    prompt: str
    title: str
    keyword1: Optional[str] = ""
    keyword2: Optional[str] = ""
    keyword3: Optional[str] = ""
    keyword4: Optional[str] = ""
    keyword5: Optional[str] = ""

class InsertPayload(BaseModel):
    schema: str
    provider: str
    generated_at: str
    count: int
    rows: List[InsertRow]

class InsertSummary(BaseModel):
    linesDetected: int
    validRows: int
    warnings: Optional[List[str]] = []

class InsertPromptsRequest(BaseModel):
    summary: InsertSummary
    payload: InsertPayload


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


# ====== GET /get_next_prompt (ดึงเฉพาะ topic แรกที่ยังไม่ใช้) ======
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
            if str(used_val).strip() == "":
                unused.append(row)

        if not unused:
            return {"prompts": []}

        # แยกตาม topic แล้วเลือกเฉพาะ topic แรก (ตามลำดับที่ปรากฏ)
        seen_topics = set()
        grouped = []
        for row in unused:
            topic = row[idx_map["topic"]]
            if topic not in seen_topics:
                seen_topics.add(topic)
                grouped.append((topic, [row]))
            else:
                for g in grouped:
                    if g[0] == topic:
                        g[1].append(row)
                        break

        if not grouped:
            return {"prompts": []}

        first_topic, selected_rows = grouped[0]

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
            range=f"{SHEET_NAME}!A2:A"  # อ่านเฉพาะ column A (rowId)
        ).execute()

        row_id_values = result.get('values', [])
        now_thai = datetime.now(pytz.timezone("Asia/Bangkok")).strftime('%Y-%m-%d %H:%M:%S')

        updates = []

        for i, row in enumerate(row_id_values):
            if not row:
                continue
            try:
                row_id = int(row[0])
            except ValueError:
                continue

            if row_id in request.rowIds:
                row_idx = i + 2  # เพราะเริ่ม A2, ต้องบวก 2 เพื่อ map ไป row จริง
                updates.append({
                    "range": f"{SHEET_NAME}!J{row_idx}:L{row_idx}",
                    "values": [["yes", request.log_id, now_thai]]
                })

        if updates:
            body = {"valueInputOption": "RAW", "data": updates}
            response = sheet_service.spreadsheets().values().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=body
            ).execute()
            logging.info("Update response: %s", response)
        else:
            logging.info("No rows matched for update.")

        return {"status": "success", "marked": len(updates)}

    except Exception as e:
        logging.exception("Error while mark_prompt_used")
        raise HTTPException(status_code=500, detail=str(e))


# ====== POST /insert_prompts ======
@app.post("/insert_prompts")
async def insert_prompts(req: InsertPromptsRequest):
    try:
        # อ่านทั้งชีตครั้งเดียว
        read = sheet_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=SHEET_NAME
        ).execute()
        values = read.get("values", [])
        if not values:
            raise HTTPException(status_code=500, detail="Sheet has no data")

        headers = values[0]
        data_rows = values[1:] if len(values) > 1 else []

        # ตรวจหัวข้อคอลัมน์ที่จำเป็น
        needed = ["rowId", "topic", "prompt", "title",
                  "keyword1", "keyword2", "keyword3", "keyword4", "keyword5",
                  "used", "log_id", "timestamp"]
        idx_map = {h: i for i, h in enumerate(headers)}
        missing = [c for c in needed if c not in idx_map]
        if missing:
            raise HTTPException(status_code=500, detail=f"Missing required columns: {missing}")

        # หา max rowId ปัจจุบัน
        max_row_id = 0
        for r in data_rows:
            try:
                rid = int(r[idx_map["rowId"]]) if len(r) > idx_map["rowId"] else 0
                if rid > max_row_id:
                    max_row_id = rid
            except ValueError:
                continue

        # เตรียม values สำหรับ append
        to_append = []
        next_id = max_row_id + 1
        for item in req.payload.rows:
            kw = [
                (item.keyword1 or "").strip(),
                (item.keyword2 or "").strip(),
                (item.keyword3 or "").strip(),
                (item.keyword4 or "").strip(),
                (item.keyword5 or "").strip(),
            ]
            row_values = [
                next_id,            # rowId (A)
                item.topic,         # topic (B)
                item.prompt,        # prompt (C)
                item.title,         # title (D)
                kw[0],              # keyword1 (E)
                kw[1],              # keyword2 (F)
                kw[2],              # keyword3 (G)
                kw[3],              # keyword4 (H)
                kw[4],              # keyword5 (I)
                "",                 # used (J)
                "",                 # log_id (K)
                "",                 # timestamp (L)
            ]
            to_append.append(row_values)
            next_id += 1

        if to_append:
            sheet_service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": to_append}
            ).execute()

        # นับจำนวนแถวที่ยังไม่ถูก mark (ใช้ข้อมูลก่อนหน้า + จำนวนที่เพิ่งเพิ่ม)
        def count_unmarked(existing_rows):
            cnt = 0
            for r in existing_rows:
                used_val = r[idx_map["used"]] if len(r) > idx_map["used"] else ""
                if str(used_val).strip().lower() != "yes":
                    cnt += 1
            return cnt

        remaining_unmarked = count_unmarked(data_rows) + len(to_append)

        return {
            "status": "success",
            "inserted": len(to_append),
            "remaining_unmarked": remaining_unmarked
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Error while insert_prompts")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == '__main__':
    from os import environ
    app.run(host='0.0.0.0', port=int(environ.get('PORT', 3000)))


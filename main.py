import os
import json
import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials
# import gspread  # ไม่ใช้แล้ว
import logging
from collections import defaultdict
import pytz
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)

# ====== SETUP GOOGLE SHEETS API ======
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_INFO = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])
creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
sheet_service = build('sheets', 'v4', credentials=creds)

SPREADSHEET_ID = '1bwjLe1Q92SP4OFqrfsqrOnn9eAtTKKCXFIpwVT2oB50'
SHEET_NAME = 'prompts'

# ===== CACHING & RATE LIMITING =====
class SheetsCache:
    def __init__(self, ttl: int = 300):  # 5 minutes TTL
        self.cache: Dict[str, Dict] = {}
        self.ttl = ttl
    
    def get(self, key: str) -> Optional[Dict]:
        if key in self.cache:
            data, timestamp = self.cache[key]['data'], self.cache[key]['timestamp']
            if time.time() - timestamp < self.ttl:
                return data
            else:
                del self.cache[key]
        return None
    
    def set(self, key: str, data: Dict):
        self.cache[key] = {'data': data, 'timestamp': time.time()}
    
    def invalidate(self, pattern: str = None):
        if pattern:
            keys_to_remove = [k for k in self.cache.keys() if pattern in k]
            for k in keys_to_remove:
                del self.cache[k]
        else:
            self.cache.clear()

# Global cache instance
sheets_cache = SheetsCache(ttl=300)

# Rate limiter
class RateLimiter:
    def __init__(self, max_requests: int = 80, window: int = 100):
        self.max_requests = max_requests
        self.window = window
        self.requests = []
    
    async def acquire(self):
        now = time.time()
        # Remove old requests outside window
        self.requests = [req_time for req_time in self.requests if now - req_time < self.window]
        
        if len(self.requests) >= self.max_requests:
            wait_time = self.window - (now - self.requests[0]) + 1
            logging.warning(f"Rate limit reached, waiting {wait_time:.2f} seconds")
            await asyncio.sleep(wait_time)
            return await self.acquire()
        
        self.requests.append(now)

rate_limiter = RateLimiter()

# ===== OPTIMIZED HELPER FUNCTIONS =====
async def get_sheet_metadata():
    """Get sheet structure (headers, range info) with caching"""
    cache_key = f"metadata_{SPREADSHEET_ID}_{SHEET_NAME}"
    cached = sheets_cache.get(cache_key)
    if cached:
        return cached
    
    await rate_limiter.acquire()
    
    try:
        # Get only first 2 rows to determine structure
        sheet = sheet_service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!1:2",
            valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        
        values = result.get("values", [])
        if not values:
            raise HTTPException(status_code=500, detail="Empty sheet")
        
        headers = values[0]
        idx_map = {h: i for i, h in enumerate(headers)}
        
        # Get sheet properties for row count
        sheet_metadata = sheet_service.spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID,
            ranges=[SHEET_NAME],
            includeGridData=False
        ).execute()
        
        sheet_info = sheet_metadata['sheets'][0]['properties']
        row_count = sheet_info.get('gridProperties', {}).get('rowCount', 1000)
        
        metadata = {
            'headers': headers,
            'idx_map': idx_map,
            'row_count': row_count,
            'required_cols': ['rowId', 'topic', 'prompt', 'used']
        }
        
        # Validate required columns
        if not all(col in idx_map for col in metadata['required_cols']):
            missing = [col for col in metadata['required_cols'] if col not in idx_map]
            raise HTTPException(status_code=500, detail=f"Missing required columns: {missing}")
        
        sheets_cache.set(cache_key, metadata)
        return metadata
        
    except Exception as e:
        logging.exception("Error getting sheet metadata")
        raise HTTPException(status_code=500, detail=f"Failed to get sheet metadata: {str(e)}")

async def get_unused_rows_by_topic():
    """Get unused rows grouped by topic with smart querying"""
    cache_key = f"unused_rows_{SPREADSHEET_ID}_{SHEET_NAME}"
    cached = sheets_cache.get(cache_key)
    if cached:
        return cached
    
    metadata = await get_sheet_metadata()
    idx_map = metadata['idx_map']
    
    await rate_limiter.acquire()
    
    try:
        # Query only necessary columns to reduce data transfer
        cols_needed = ['rowId', 'topic', 'prompt', 'used', 'title'] + \
                     [h for h in metadata['headers'] if h.lower().startswith('keyword')]
        
        # Create range for specific columns
        col_letters = [chr(ord('A') + idx_map[col]) for col in cols_needed if col in idx_map]
        range_query = f"{SHEET_NAME}!A2:{max(col_letters)}{metadata['row_count']}"
        
        sheet = sheet_service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_query,
            valueRenderOption="UNFORMATTED_VALUE",
            majorDimension="ROWS"
        ).execute()
        
        values = result.get("values", [])
        
        def get_cell(row, col_name):
            if col_name not in idx_map:
                return ""
            col_idx = idx_map[col_name]
            if col_idx >= len(row):
                return ""
            return str(row[col_idx]).strip()
        
        # Filter unused rows and group by topic
        unused_by_topic = {}
        for row_idx, row in enumerate(values, start=2):
            if not row:  # Skip empty rows
                continue
                
            used_status = get_cell(row, 'used')
            if used_status.lower() == "yes":  # Skip used rows (changed from != "")
                continue
            
            topic = get_cell(row, 'topic')
            if not topic:  # Skip rows without topic
                continue
            
            row_data = {
                'row_idx': row_idx,
                'rowId': int(get_cell(row, 'rowId') or 0),
                'topic': topic,
                'prompt': get_cell(row, 'prompt'),
                'title': get_cell(row, 'title'),
                'raw_row': row
            }
            
            if topic not in unused_by_topic:
                unused_by_topic[topic] = []
            unused_by_topic[topic].append(row_data)
        
        # Cache for shorter time since this changes frequently
        sheets_cache.set(cache_key, unused_by_topic)
        return unused_by_topic
        
    except Exception as e:
        logging.exception("Error getting unused rows")
        raise HTTPException(status_code=500, detail=f"Failed to get unused rows: {str(e)}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup - warm up cache
    try:
        await get_sheet_metadata()
        logging.info("Cache warmed up successfully")
    except Exception as e:
        logging.warning(f"Failed to warm up cache: {e}")
    
    yield
    
    # Shutdown - clear cache
    sheets_cache.invalidate()
    
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ====== Models ======
class ImageLog(BaseModel):
    id: str
    filename: str
    title: str
    keywords: List[str]
    prompt: str

class InsertRow(BaseModel):
    topic: str
    prompt: str
    title: str
    keyword1: Optional[str] = ""
    keyword2: Optional[str] = ""
    keyword3: Optional[str] = ""
    keyword4: Optional[str] = ""
    keyword5: Optional[str] = ""
    keyword6: Optional[str] = ""
    keyword7: Optional[str] = ""
    keyword8: Optional[str] = ""
    keyword9: Optional[str] = ""
    keyword10: Optional[str] = ""

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

class LockPayload(BaseModel):
    rowId: int
    log_id: str

class ClearPayload(BaseModel):
    rowId: int

class MarkPromptRequest(BaseModel):
    rowIds: List[int]
    log_id: str

# ====== FILE MANAGEMENT APIs ======
@app.get("/load/{filename}")
async def load_file(filename: str):
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            content = file.read()
        return {"status": "success", "content": content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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

@app.get("/list_uploads")
async def list_uploads():
    try:
        if not os.path.exists('uploads'):
            return []
        files = os.listdir('uploads')
        return files
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download/zip/{filename}")
async def download_zip(filename: str):
    file_path = os.path.join('uploads', filename)
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=filename, media_type='application/zip')
    else:
        raise HTTPException(status_code=404, detail="File not found")

# ====== REMOVED: Upload Logs API (deprecated) ======
# @app.post("/upload/log")
# async def upload_logs(logs: List[ImageLog]):
#     # This endpoint has been deprecated and removed

# ====== ROBUST mark_prompt_used (column-safe) ======
def col_idx_to_a1(col_idx_zero_based: int) -> str:
    # 0 -> A, 25 -> Z, 26 -> AA ...
    n = col_idx_zero_based + 1
    letters = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return letters
    
# ===== OPTIMIZED PROMPT MANAGEMENT APIs =====
@app.get("/get_next_prompt")
async def get_next_prompt():
    try:
        # อ่าน metadata + ทั้งชีต (เพื่อคงลำดับแถวจริง)
        metadata = await get_sheet_metadata()
        headers = metadata["headers"]
        idx_map  = metadata["idx_map"]

        # ตรวจคอลัมน์ขั้นต่ำที่ต้องมี
        required = ["rowId", "topic", "prompt", "title", "used"]
        missing  = [c for c in required if c not in idx_map]
        if missing:
            raise HTTPException(status_code=500, detail=f"Missing required columns: {missing}")

        await rate_limiter.acquire()
        read = sheet_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=SHEET_NAME
        ).execute()
        values = read.get("values", [])
        if not values:
            return {"prompts": []}

        # แถวข้อมูลจริงเริ่มหลัง header
        data_rows = values[1:] if len(values) > 1 else []

        # เตรียมตัวช่วยอ่านค่าเซลล์ตามชื่อคอลัมน์
        def cell(row, col):
            i = idx_map.get(col)
            if i is None or i >= len(row):
                return ""
            v = row[i]
            return "" if v is None else str(v).strip()

        # หา index ของแถวแรกที่ used ว่างจริง
        first_idx = None
        for i, row in enumerate(data_rows):
            if cell(row, "used") == "":
                first_idx = i
                break

        if first_idx is None:
            return {"prompts": []}

        # อ่าน topic ของแถวแรกนั้น
        target_topic = cell(data_rows[first_idx], "topic")

        # เก็บบล็อกแถวติดกันที่ topic เหมือนกัน และ used ว่าง
        block_rows = []
        i = first_idx
        while i < len(data_rows):
            row = data_rows[i]
            if cell(row, "used") != "":
                break
            if cell(row, "topic") != target_topic:
                break
            block_rows.append((i, row))  # เก็บ (indexภายในdata_rows, raw_row)
            i += 1

        if not block_rows:
            return {"prompts": []}

        # เตรียมรายชื่อคอลัมน์ keyword* ตามลำดับจริงในชีต
        keyword_cols = [h for h in headers if isinstance(h, str) and h.strip().lower().startswith("keyword")]
        keyword_cols.sort(key=lambda h: idx_map.get(h, 10**9))

        # ประกอบผลลัพธ์
        output = []
        for i, row in block_rows:
            # ดึงค่าหลัก
            row_id   = cell(row, "rowId")
            topic    = cell(row, "topic")
            prompt   = cell(row, "prompt")
            title    = cell(row, "title") or (topic or prompt)[:70].strip()

            # keywords จากคอลัมน์จริง (ลำดับตามคอลัมน์)
            seen, kws = set(), []
            for kc in keyword_cols:
                v = cell(row, kc)
                if v and v not in seen:
                    seen.add(v); kws.append(v)

            # เติมจาก prompt ให้ครบ 10 ถ้ายังไม่ถึง
            if len(kws) < 10 and prompt:
                for token in prompt.split():
                    t = token.strip(",.;:()[]{}'\"").lower()
                    if t and t not in seen:
                        seen.add(t); kws.append(t)
                        if len(kws) >= 10:
                            break

            output.append({
                "rowId": int(row_id) if str(row_id).isdigit() else row_id,
                "topic": topic,
                "prompt": prompt,
                "title": title,
                "keywords": kws[:10]
            })

        return {"prompts": output}

    except Exception as e:
        logging.exception("Error in get_next_prompt (contiguous block)")
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/mark_prompt_locked")
async def mark_prompt_locked(p: LockPayload):
    try:
        # เคลียร์ cache เฉพาะ key ที่เกี่ยวข้อง (ถ้ามีระบบแบ่ง key)
        sheets_cache.invalidate("unused_rows")

        unused_by_topic = await get_unused_rows_by_topic()

        # หา topic ที่มี rowId เป้าหมาย
        target_topic = None
        for topic, rows in unused_by_topic.items():
            if any(row["rowId"] == p.rowId for row in rows):
                target_topic = topic
                break

        if not target_topic:
            raise HTTPException(404, "rowId not found in unused rows")

        metadata = await get_sheet_metadata()
        idx_map = metadata["idx_map"]

        # ยืนยันคีย์คอลัมน์
        required = ["used", "log_id"]
        missing = [k for k in required if k not in idx_map]
        if missing:
            raise HTTPException(500, f"Missing columns in header: {', '.join(missing)}")

        used_col = col_idx_to_a1(idx_map["used"])
        log_col  = col_idx_to_a1(idx_map["log_id"])
        time_col = col_idx_to_a1(idx_map["timestamp"]) if "timestamp" in idx_map else None

        now_thai = datetime.now(pytz.timezone("Asia/Bangkok")).strftime("%Y-%m-%d %H:%M:%S")

        updates = []
        for row_data in unused_by_topic[target_topic]:
            row_num = row_data["row_idx"]  # ควรเป็นเลขแถวจริงในชีต (เริ่มนับจาก 1)

            # ข้ามแถวที่ล็อกอยู่แล้ว (กันเขียนซ้ำ)
            current_used = (row_data.get("used") or "").strip().upper()
            if current_used == "LOCKED":
                continue

            if time_col:
                # เขียน 3 คอลัมน์ในช่วงเดียว
                updates.append({
                    "range": f"{SHEET_NAME}!{used_col}{row_num}:{time_col}{row_num}",
                    "values": [["LOCKED", p.log_id, now_thai]]
                })
            else:
                # เขียน 2 คอลัมน์ (ไม่มี timestamp)
                updates.append({
                    "range": f"{SHEET_NAME}!{used_col}{row_num}:{log_col}{row_num}",
                    "values": [["LOCKED", p.log_id]]
                })

        if updates:
            await rate_limiter.acquire()
            body = {"valueInputOption": "RAW", "data": updates}
            sheet_service.spreadsheets().values().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=body
            ).execute()

            sheets_cache.invalidate()  # เคลียร์ cache รวม หลังอัปเดต
        return {"status": "ok", "locked_cells": len(updates)}

    except Exception as e:
        logging.exception("Error in mark_prompt_locked")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/clear_prompt_mark")
async def clear_prompt_mark(p: ClearPayload):
    try:
        # This needs fresh data, so get it without cache
        sheets_cache.invalidate()
        
        await rate_limiter.acquire()
        
        # Get minimal data needed - only status columns
        metadata = await get_sheet_metadata()
        idx_map = metadata['idx_map']
        
        needed_cols = ['rowId', 'topic', 'used']
        col_letters = [chr(ord('A') + idx_map[col]) for col in needed_cols]
        range_query = f"{SHEET_NAME}!A2:{max(col_letters)}{metadata['row_count']}"
        
        sheet = sheet_service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_query
        ).execute()
        
        values = result.get("values", [])
        if not values:
            raise HTTPException(404, "No data found")
        
        # Find target topic
        target_topic = None
        for i, row in enumerate(values, start=2):
            if len(row) <= idx_map['rowId']:
                continue
            try:
                rid = int(row[idx_map['rowId']])
                if rid == p.rowId:
                    target_topic = row[idx_map['topic']] if len(row) > idx_map['topic'] else ""
                    break
            except (ValueError, IndexError):
                continue
        
        if not target_topic:
            raise HTTPException(404, "rowId not found")
        
        # Prepare updates for topic rows that need clearing
        updates = []
        clear_cols = ['used', 'log_id', 'timestamp']
        
        for i, row in enumerate(values, start=2):
            if len(row) <= idx_map['topic']:
                continue
                
            topic = row[idx_map['topic']]
            used = row[idx_map['used']] if len(row) > idx_map['used'] else ""
            used = used.strip().upper()
            
            if topic == target_topic and used in ("", "LOCKED", "FAILED"):
                for col in clear_cols:
                    if col in idx_map:
                        col_letter = chr(ord('A') + idx_map[col])
                        updates.append({
                            "range": f"{SHEET_NAME}!{col_letter}{i}",
                            "values": [[""]]
                        })
        
        if updates:
            await rate_limiter.acquire()
            body = {"valueInputOption": "RAW", "data": updates}
            sheet_service.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
            
            # Clear cache
            sheets_cache.invalidate()

    except Exception as e:
        logging.exception("Error in clear_prompt_mark")
        raise HTTPException(status_code=500, detail=str(e))
        
# ====== OPTIMIZED mark_prompt_used ======
@app.post("/mark_prompt_used")
async def mark_prompt_used(request: MarkPromptRequest):
    try:
        sheets_cache.invalidate()
        await rate_limiter.acquire()

        metadata = await get_sheet_metadata()
        idx_map = metadata["idx_map"]  # ควรมี 'row_id', 'used', 'log_id', 'timestamp'
        row_count = metadata["row_count"]

        # ตรวจครบถ้วน
        required = ["rowId", "used", "log_id", "timestamp"]
        missing = [k for k in required if k not in idx_map]
        if missing:
            raise HTTPException(
                status_code=500,
                detail=f"Missing columns in header: {', '.join(missing)}"
            )

        # คอลัมน์ A1 จาก index (zero-based)
        row_id_col = col_idx_to_a1(idx_map["rowId"])
        used_col   = col_idx_to_a1(idx_map["used"])
        log_col    = col_idx_to_a1(idx_map["log_id"])
        time_col   = col_idx_to_a1(idx_map["timestamp"])

        # ดึงเฉพาะคอลัมน์ row_id แบบไดนามิก (ไม่ฮาร์ดโค้ด A)
        result = sheet_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!{row_id_col}2:{row_id_col}{row_count}"
        ).execute()

        row_id_values = result.get("values", [])
        now_thai = datetime.now(pytz.timezone("Asia/Bangkok")).strftime("%Y-%m-%d %H:%M:%S")

        updates = []
        for i, row in enumerate(row_id_values):
            if not row:
                continue
            try:
                row_id = int(row[0])
            except ValueError:
                continue

            if row_id in request.rowIds:
                row_idx = i + 2  # เพราะเริ่มจากแถว 2
                updates.append({
                    "range": f"{SHEET_NAME}!{used_col}{row_idx}:{time_col}{row_idx}",
                    "values": [["yes", request.log_id, now_thai]]
                })

        if updates:
            await rate_limiter.acquire()
            body = {"valueInputOption": "RAW", "data": updates}
            resp = sheet_service.spreadsheets().values().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=body
            ).execute()
            logging.info("Update response: %s", resp)
            sheets_cache.invalidate()
        else:
            logging.info("No rows matched for update.")

        return {"status": "success", "marked": len(updates)}

    except Exception as e:
        logging.exception("Error while mark_prompt_used")
        raise HTTPException(status_code=500, detail=str(e))

# ====== OPTIMIZED insert_prompts ======  
@app.post("/insert_prompts")
async def insert_prompts(req: InsertPromptsRequest):
    try:
        # เคลียร์ cache เพราะจะมีการเพิ่มข้อมูล
        sheets_cache.invalidate()
        await rate_limiter.acquire()

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
        # ทำ idx_map จากหัวตารางจริง
        idx_map = {h: i for i, h in enumerate(headers)}

        # ตรวจคอลัมน์จำเป็นขั้นต่ำ (ไม่บังคับ keyword6–10)
        required_min = ["rowId", "topic", "prompt", "title", "used", "log_id", "timestamp"]
        missing_min = [c for c in required_min if c not in idx_map]
        if missing_min:
            raise HTTPException(status_code=500, detail=f"Missing required columns: {missing_min}")

        # หา keyword columns จากหัวจริงทั้งหมด แล้วเรียงตามตำแหน่ง
        keyword_cols = [h for h in headers if isinstance(h, str) and h.strip().lower().startswith("keyword")]
        keyword_cols.sort(key=lambda h: idx_map[h])

        # หา max rowId ปัจจุบัน
        max_row_id = 0
        rowid_idx = idx_map["rowId"]
        for r in data_rows:
            try:
                rid = int(r[rowid_idx]) if len(r) > rowid_idx else 0
                if rid > max_row_id:
                    max_row_id = rid
            except ValueError:
                continue

        # helper: ดึงค่าคีย์เวิร์ด 1..10 จาก payload (ไม่ครบให้ค่าว่าง)
        def extract_keywords(item):
            kws = []
            for i in range(1, 11):  # รองรับ keyword1..keyword10
                val = getattr(item, f"keyword{i}", None)
                kws.append((val or "").strip())
            return kws

        # เตรียม values สำหรับ append (ความยาวแถว = จำนวน headers จริง)
        to_append = []
        next_id = max_row_id + 1
        used_idx = idx_map["used"]
        log_idx = idx_map["log_id"]
        ts_idx = idx_map["timestamp"]

        for item in req.payload.rows:
            row_vals = [""] * len(headers)

            # base fields
            row_vals[rowid_idx] = next_id
            if "topic" in idx_map:   row_vals[idx_map["topic"]] = item.topic
            if "prompt" in idx_map:  row_vals[idx_map["prompt"]] = item.prompt
            if "title" in idx_map:   row_vals[idx_map["title"]] = item.title

            # map keywords ลงตามคอลัมน์ keyword ที่มีจริงในชีต
            payload_kws = extract_keywords(item)    # ยาว 10 เสมอ (อาจเป็น "" ถ้าไม่มี)
            for i, col_name in enumerate(keyword_cols):
                # i อาจมากกว่า 9 ถ้าชีตมี keyword เกิน 10; ตัดที่ payload ที่มี
                if i >= len(payload_kws):
                    break
                col_idx = idx_map[col_name]
                row_vals[col_idx] = payload_kws[i]

            # used/log_id/timestamp เว้นว่าง (ให้ flow อื่นไปกรอก)
            row_vals[used_idx] = ""
            row_vals[log_idx] = ""
            row_vals[ts_idx] = ""

            to_append.append(row_vals)
            next_id += 1

        if to_append:
            await rate_limiter.acquire()
            sheet_service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": to_append}
            ).execute()
            sheets_cache.invalidate()

        # นับแถวที่ยังไม่ถูก mark (ยังไม่ใช่ "yes")
        def count_unmarked(existing_rows):
            cnt = 0
            for r in existing_rows:
                used_val = r[used_idx] if len(r) > used_idx else ""
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





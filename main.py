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

# ===== OPTIMIZED PROMPT MANAGEMENT APIs =====

@app.get("/get_next_prompt")
async def get_next_prompt():
    try:
        unused_by_topic = await get_unused_rows_by_topic()
        
        if not unused_by_topic:
            return {"prompts": []}
        
        # Get first topic
        first_topic = next(iter(unused_by_topic.keys()))
        selected_rows = unused_by_topic[first_topic]
        
        metadata = await get_sheet_metadata()
        headers = metadata['headers']
        idx_map = metadata['idx_map']
        
        # Prepare keyword columns
        keyword_cols = [h for h in headers if h.lower().startswith("keyword")]
        
        def get_cell(row, col_name):
            if col_name not in idx_map:
                return ""
            col_idx = idx_map[col_name]
            if col_idx >= len(row):
                return ""
            return str(row[col_idx]).strip()
        
        # Build output
        output = []
        for row_data in selected_rows:
            row = row_data['raw_row']
            
            # Title with fallback
            title = row_data['title']
            if not title:
                base = row_data['topic'] or row_data['prompt']
                title = (base[:70]).strip()
            
            # Collect keywords
            kws = []
            for kc in keyword_cols:
                v = get_cell(row, kc)
                if v:
                    kws.append(v.strip())
            
            # Remove duplicates
            seen = set()
            kws = [k for k in kws if k and not (k in seen or seen.add(k))]
            
            # Add from prompt if needed
            if len(kws) < 10:
                for token in row_data['prompt'].split():
                    t = token.strip(",.;:()[]{}'\"").lower()
                    if t and t not in seen:
                        seen.add(t)
                        kws.append(t)
                        if len(kws) >= 10:
                            break
            
            output.append({
                "rowId": row_data['rowId'],
                "topic": row_data['topic'],
                "prompt": row_data['prompt'],
                "title": title,
                "keywords": kws[:10]
            })
        
        return {"prompts": output}
        
    except Exception as e:
        logging.exception("Error in get_next_prompt")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/mark_prompt_locked")
async def mark_prompt_locked(p: LockPayload):
    try:
        # Invalidate cache since we're making changes
        sheets_cache.invalidate("unused_rows")
        
        unused_by_topic = await get_unused_rows_by_topic()
        
        # Find topic for the rowId
        target_topic = None
        for topic, rows in unused_by_topic.items():
            if any(row['rowId'] == p.rowId for row in rows):
                target_topic = topic
                break
        
        if not target_topic:
            raise HTTPException(404, "rowId not found in unused rows")
        
        metadata = await get_sheet_metadata()
        idx_map = metadata['idx_map']
        
        # Prepare batch update for all unused rows in the topic
        updates = []
        for row_data in unused_by_topic[target_topic]:
            row_num = row_data['row_idx']
            used_col = chr(ord('A') + idx_map['used'])
            log_col = chr(ord('A') + idx_map['log_id'])
            
            updates.append({
                "range": f"{SHEET_NAME}!{used_col}{row_num}",
                "values": [["LOCKED"]]
            })
            updates.append({
                "range": f"{SHEET_NAME}!{log_col}{row_num}",
                "values": [[p.log_id]]
            })
        
        if updates:
            await rate_limiter.acquire()
            body = {"valueInputOption": "RAW", "data": updates}
            sheet_service.spreadsheets().values().batchUpdate(
                spreadsheetId=SPREADSHEET_ID, 
                body=body
            ).execute()
            
            # Invalidate cache after update
            sheets_cache.invalidate()
        
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
        
# ====== OPTIMIZED mark_prompt_used ======
@app.post("/mark_prompt_used")
async def mark_prompt_used(request: MarkPromptRequest):
    try:
        # Invalidate cache since we're making changes
        sheets_cache.invalidate()
        
        await rate_limiter.acquire()
        
        metadata = await get_sheet_metadata()
        idx_map = metadata['idx_map']
        
        # Get only rowId column for efficient lookup
        result = sheet_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A2:A{metadata['row_count']}"
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
                row_idx = i + 2  # เพราะเริ่ม A2
                
                # Assume columns: used=J, log_id=K, timestamp=L (adjust as needed)
                used_col = chr(ord('A') + idx_map.get('used', 9))  # default J
                log_col = chr(ord('A') + idx_map.get('log_id', 10))  # default K  
                time_col = chr(ord('A') + idx_map.get('timestamp', 11))  # default L
                
                updates.append({
                    "range": f"{SHEET_NAME}!{used_col}{row_idx}:{time_col}{row_idx}",
                    "values": [["yes", request.log_id, now_thai]]
                })

        if updates:
            await rate_limiter.acquire()
            body = {"valueInputOption": "RAW", "data": updates}
            response = sheet_service.spreadsheets().values().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=body
            ).execute()
            logging.info("Update response: %s", response)
            
            # Invalidate cache after update
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
        # Invalidate cache since we're adding data
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
            await rate_limiter.acquire()
            sheet_service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": to_append}
            ).execute()
            
            # Invalidate cache after insert
            sheets_cache.invalidate()

        # นับจำนวนแถวที่ยังไม่ถูก mark
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

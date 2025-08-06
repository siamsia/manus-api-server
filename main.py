from flask import Flask, request, jsonify
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

app = Flask(__name__)

SCOPES = ['https://www.googleapis.com/auth/drive']
service_account_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])
creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)

# ====== SETUP Google Drive API ======
#SERVICE_ACCOUNT_FILE = 'service_account.json'  # <-- อัปโหลดไฟล์ JSON นี้ไปด้วย
FOLDER_ID = '1fecni5SG7jN97nlWpYePpRaF3XgES8f2'  # <-- ใส่ Folder ID ของ Google Drive ที่ต้องการอัปโหลดไฟล์

drive_service = build('drive', 'v3', credentials=creds)


# ====== API: Load todo.txt / image_history.txt ======
@app.route('/load/<filename>', methods=['GET'])
def load_file(filename):
    try:
        with open(filename, 'r') as file:
            content = file.read()
        return jsonify({'status': 'success', 'content': content})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


# ====== API: Save todo.txt / image_history.txt ======
@app.route('/save/<filename>', methods=['POST'])
def save_file(filename):
    data = request.json
    content = data.get('content', '')
    try:
        with open(filename, 'w') as file:
            file.write(content)
        return jsonify({
            'status': 'success',
            'message': f'{filename} saved successfully!'
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


# ====== API: Upload ZIP file to Google Drive ======
@app.route('/upload_zip', methods=['POST'])
def upload_zip():
    data = request.json
    zip_file_path = data.get('filepath', '')
    if not os.path.exists(zip_file_path):
        return jsonify({'status': 'error', 'message': 'File not found'})
    try:
        file_metadata = {
            'name': os.path.basename(zip_file_path),
            'parents': [FOLDER_ID]
        }
        media = MediaFileUpload(zip_file_path, mimetype='application/zip')
        file = drive_service.files().create(body=file_metadata,
                                            media_body=media,
                                            fields='id').execute()
        return jsonify({'status': 'success', 'file_id': file.get("id")})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


if __name__ == '__main__':
    from os import environ
    app.run(host='0.0.0.0', port=int(environ.get('PORT', 3000)))


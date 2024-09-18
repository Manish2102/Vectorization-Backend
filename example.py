import os
import requests
import base64
from flask import Flask, request, jsonify, render_template_string
import logging
from flask_cors import CORS  # Import CORS

app = Flask(__name__)
CORS(app)  # Apply CORS to the entire app

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load sensitive credentials from environment variables
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', 'https://adb-1620865038680305.5.azuredatabricks.net')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN', 'dapibbaaa71fcd3f5fd3612a6a37120509d2-3')
JOB_ID = os.getenv('JOB_ID', '595512840231112')  # Your existing Databricks Job ID

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

# Paths for data storage
STRUCTURED_PATH = "/FileStore/Group-6_Data/Structured-data"  # Path for structured data
UNSTRUCTURED_PATH = "/FileStore/Group-6_Data/Unstructured-data"  # Path for unstructured data

# Allowed file extensions
STRUCTURED_EXTENSIONS = ['csv', 'xls', 'xlsx', 'png']
UNSTRUCTURED_EXTENSIONS = ['pdf', 'doc', 'docx', 'json', '.txt']

@app.route('/', methods=["GET"])
def index():
    return 'hi'

def is_valid_file(file, data_type):
    extension = file.filename.rsplit('.', 1)[-1].lower()
    if data_type == 'structured':
        return extension in STRUCTURED_EXTENSIONS
    elif data_type == 'unstructured':
        return extension in UNSTRUCTURED_EXTENSIONS
    return False

def upload_file_to_dbfs(file, data_type):
    # Determine the destination path based on data type
    if data_type == 'structured':
        path = STRUCTURED_PATH
    elif data_type == 'unstructured':
        path = UNSTRUCTURED_PATH
    else:
        raise ValueError("Invalid data type")

    # Convert the file to binary
    file_content = file.read()

    # Upload the file to DBFS
    dbfs_upload_url = f"{DATABRICKS_HOST}/api/2.0/dbfs/put"
    payload = {
        "path": path + "/" + file.filename,  # Include filename in the path
        "contents": base64.b64encode(file_content).decode('utf-8'),
        "overwrite": True
    }
    response = requests.post(dbfs_upload_url, headers=headers, json=payload)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to upload file to DBFS: {response.status_code}, {response.text}")

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files['file']
    data_type = request.form.get('data-type')

    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400

    if not is_valid_file(file, data_type):
        allowed_extensions = ', '.join(STRUCTURED_EXTENSIONS) if data_type == 'structured' else ', '.join(UNSTRUCTURED_EXTENSIONS)
        return jsonify({"error": f"Invalid file type for {data_type} data. Allowed types are: {allowed_extensions}"}), 400

    try:
        # Upload file to DBFS
        response = upload_file_to_dbfs(file, data_type)
        return jsonify({
            "message": f"File uploaded successfully to {STRUCTURED_PATH if data_type == 'structured' else UNSTRUCTURED_PATH}/{file.filename}",
            "response": response
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/trigger-job', methods=['POST'])
def trigger_job():
    run_job_url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    run_payload = {
        "job_id": JOB_ID,
    }
    
    response = requests.post(run_job_url, headers=headers, json=run_payload)
    
    if response.status_code == 200:
        return response.json(), 200
    else:
        raise Exception(f"Failed to trigger job: {response.status_code}, {response.text}")

@app.route('/list-files/structured', methods=['GET'])
def list_files_structured():
    path = request.args.get('path', STRUCTURED_PATH)  # Default path for listing files
    
    dbfs_list_url = f"{DATABRICKS_HOST}/api/2.0/dbfs/list"
    params = {
        "path": path
    }
    response = requests.get(dbfs_list_url, headers=headers, params=params)

    if response.status_code == 200:
        return jsonify(response.json()), 200
    else:
        return jsonify({"error": f"Failed to list files: {response.status_code}, {response.text}"}), 500

@app.route('/list-files/unstructured', methods=['GET'])
def list_files_unstructured():
    path = request.args.get('path', UNSTRUCTURED_PATH)  # Default path for listing files
    
    dbfs_list_url = f"{DATABRICKS_HOST}/api/2.0/dbfs/list"
    params = {
        "path": path
    }
    response = requests.get(dbfs_list_url, headers=headers, params=params)

    if response.status_code == 200:
        return jsonify(response.json()), 200
    else:
        return jsonify({"error": f"Failed to list files: {response.status_code}, {response.text}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

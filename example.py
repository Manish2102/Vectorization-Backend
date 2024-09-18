from flask import Flask, request, jsonify, render_template_string
import os
import requests
import base64
from flask_cors import CORS  # Import CORS

app = Flask(__name__)
CORS(app)  # Apply CORS to the entire app

# Load sensitive credentials from environment variables
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', 'https://adb-1620865038680305.5.azuredatabricks.net')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN', 'dapibbaaa71fcd3f5fd3612a6a37120509d2-3')

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

# Paths for data storage
STRUCTURED_PATH = "/FileStore/Group-6_Data/Structured-data"  # Path for structured data
UNSTRUCTURED_PATH = "/FileStore/Group-6_Data/Unstructured-data"  # Path for unstructured data

# Allowed file extensions
STRUCTURED_EXTENSIONS = ['csv', 'xls', 'xlsx', 'png']
UNSTRUCTURED_EXTENSIONS = ['pdf', 'doc', 'docx', 'json']

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
    print(f"File size in bytes: {len(file_content)}")  # Debugging line

    # Upload the file to DBFS
    dbfs_upload_url = f"{DATABRICKS_HOST}/api/2.0/dbfs/put"
    payload = {
        "path": path + "/" + file.filename,  # Include filename in the path
        "contents": base64.b64encode(file_content).decode('utf-8'),
        "overwrite": True
    }
    response = requests.post(dbfs_upload_url, headers=headers, json=payload)
    print(f"Response status code: {response.status_code}")  # Debugging line
    print(f"Response text: {response.text}")  # Debugging line

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
        return jsonify({"error": f"Invalid file type for {data_type} data. Allowed types are: {', '.join(STRUCTURED_EXTENSIONS) if data_type == 'structured' else ', '.join(UNSTRUCTURED_EXTENSIONS)}"}), 400

    try:
        # Upload file to DBFS
        response = upload_file_to_dbfs(file, data_type)
        return jsonify({"message": f"File uploaded successfully to {STRUCTURED_PATH if data_type == 'structured' else UNSTRUCTURED_PATH}/{file.filename}", "response": response}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/list-files', methods=['GET'])
def list_files():
    path = request.args.get('path', STRUCTURED_PATH)  # Default path for listing files
    
    # List files in DBFS
    dbfs_list_url = f"{DATABRICKS_HOST}/api/2.0/dbfs/list"
    params = {
        "path": path
    }
    response = requests.get(dbfs_list_url, headers=headers, params=params)
    
    print(f"Response status code: {response.status_code}")  # Debugging line
    print(f"Response text: {response.text}")  # Debugging line

    if response.status_code == 200:
        return jsonify(response.json()), 200
    else:
        return jsonify({"error": f"Failed to list files: {response.status_code}, {response.text}"}), 500

if __name__ == '_main_':
    app.run(debug=True, host='0.0.0.0', port=5000)
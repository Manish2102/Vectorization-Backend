# app.py
from flask import Flask, request, jsonify
import requests
import base64
import os
import tempfile

app = Flask(__name__)

# Databricks configuration
DATABRICKS_INSTANCE = 'https://adb-1620865038680305.5.azuredatabricks.net'
API_TOKEN = 'dapibbaaa71fcd3f5fd3612a6a37120509d2-3'
MOUNT_PATH = 'https://adb-1620865038680305.5.azuredatabricks.net/explore/data/volumes/main/default/all_types_data_grp6?o=1620865038680305'  # Adjusted to a relative DBFS path

@app.route('/api/v1/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    # Save the file to a temporary directory
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(file.read())
        temp_file_path = temp_file.name

    response = upload_to_dbfs(temp_file_path, file.filename)
    
    os.remove(temp_file_path)  # Clean up temporary file

    if response.status_code == 200:
        return jsonify({'message': 'File uploaded successfully'}), 200
    else:
        return jsonify({'error': 'Failed to upload file', 'details': response.json()}), response.status_code

def upload_to_dbfs(file_path, file_name):
    url = f'{DATABRICKS_INSTANCE}/api/2.0/dbfs/put'
    headers = {
        'Authorization': f'Bearer {API_TOKEN}',
        'Content-Type': 'application/json'
    }
    with open(file_path, 'rb') as f:
        file_contents = base64.b64encode(f.read()).decode('utf-8')
    
    payload = {
        'path': f'{MOUNT_PATH}/{file_name}',
        'contents': file_contents,
        'overwrite': True
    }
    return requests.post(url, json=payload, headers=headers)

@app.route('/api/v1/list-files', methods=['GET'])
def list_files():
    url = f'{DATABRICKS_INSTANCE}/api/2.0/dbfs/list'
    headers = {
        'Authorization': f'Bearer {API_TOKEN}',
        'Content-Type': 'application/json'
    }
    params = {
        'path': MOUNT_PATH
    }
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        files = response.json().get('files', [])
        file_paths = [file['path'] for file in files]
        return jsonify({'files': file_paths}), 200
    else:
        return jsonify({'error': 'Failed to list files', 'details': response.json()}), response.status_code

if __name__ == '__main__':
    app.run(debug=True)

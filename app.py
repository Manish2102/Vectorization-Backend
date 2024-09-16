import os
import requests
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# Set Databricks configuration (replace with your details)
DATABRICKS_INSTANCE = "https://adb-1620865038680305.5.azuredatabricks.net"
DBFS_API_ENDPOINT = f"{DATABRICKS_INSTANCE}/api/2.0/dbfs/put"
DATABRICKS_TOKEN = "dapibbaaa71fcd3f5fd3612a6a37120509d2-3"

# Helper function to upload file to DBFS
def upload_file_to_dbfs(file, path):
    # Prepare headers for the Databricks API
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }

    # Read file content
    file_content = file.read()

    # Prepare the payload for the request
    data = {
        "path": path,
        "overwrite": "true"  # Set to True if you want to overwrite existing files
    }
    files = {
        'file': file_content
    }

    # Perform the API request
    response = requests.post(DBFS_API_ENDPOINT, headers=headers, data=data, files=files)

    # Check if upload was successful
    if response.status_code == 200:
        return {"status": "success", "message": "File uploaded successfully!"}
    else:
        return {"status": "error", "message": f"Failed to upload file: {response.text}"}

# Route to display the upload form
@app.route('/')
def index():
    return '''
    <!doctype html>
    <title>Upload File to Databricks DBFS</title>
    <h1>Upload File</h1>
    <form action="/api/v1/upload" method="POST" enctype="multipart/form-data">
        <label for="path">DBFS Path:</label><br>
        <input type="text" id="path" name="path" value="/dbfs:/filestore/Group-6_Data"><br><br>
        <input type="file" name="file"><br><br>
        <input type="submit" value="Upload">
    </form>
    '''

# Route for uploading file
@app.route('/api/v1/upload', methods=['POST'])
def upload():
    # Check if 'file' and 'path' are in the request
    if 'file' not in request.files or 'path' not in request.form:
        return jsonify({"status": "error", "message": "Missing file or path in the request"}), 400

    file = request.files['file']
    dbfs_path = request.form['path']

    # Check for file validity
    if file.filename == '':
        return jsonify({"status": "error", "message": "No file selected for uploading"}), 400

    # Ensure path starts with '/dbfs:/'
    if not dbfs_path.startswith('/dbfs:/'):
        return jsonify({"status": "error", "message": "Invalid DBFS path. Path must start with '/dbfs:/'"}), 400

    # Construct full path
    full_dbfs_path = f"{dbfs_path}/{file.filename}"

    # Upload file to DBFS
    result = upload_file_to_dbfs(file, full_dbfs_path)

    # Return upload status
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)

from datetime import datetime, timezone
import json
import os
import time
import requests
import base64
from flask import Flask, render_template, request, jsonify, render_template_string
import logging
from flask_cors import CORS


app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # Set maximum file size to 500 MB

# Enable CORS for all routes
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load sensitive credentials from environment variables
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', 'https://adb-1620865038680305.5.azuredatabricks.net')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN', 'dapibbaaa71fcd3f5fd3612a6a37120509d2-3')
JOB_ID_STRUCTURED = os.getenv('JOB_ID_STRUCTURED', '318641275335090')  # Job ID for structured data upload
JOB_ID_UNSTRUCTURED = os.getenv('JOB_ID_UNSTRUCTURED', '78575902542513')  # Job ID for unstructured data upload

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

# Paths for data storage
STRUCTURED_PATH = "/FileStore/Group-6_Data/Structured-data"
UNSTRUCTURED_PATH = "/FileStore/Group-6_Data/Unstructured-data"

# Allowed file extensionss
STRUCTURED_EXTENSIONS = ['csv', 'xls', 'xlsx', 'png']
UNSTRUCTURED_EXTENSIONS = ['pdf', 'doc', 'docx', 'json']


import requests
import json

def invoke_databricks_notebook(notebook_path, query_text):
    api_url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit"
    payload = {
        "run_name": "InvokeNotebookViaAPI",
        "existing_cluster_id": "0913-074703-3qd31gjt",  # Replace with your Databricks cluster ID
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": {
                "query_text": query_text  # Pass the parameter to the notebook
            }
        }
    }

    # Define the headers for the API request
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    # Make the POST request to invoke the notebook
    response = requests.post(api_url, headers=headers, data=json.dumps(payload))

    # Check for a successful response
    if response.status_code == 200:
        print("Notebook invoked successfully.")
        print(response)
        return response.json()
    else:
        print(f"Failed to invoke notebook. Status Code: {response.status_code}, Error: {response.text}")
        return None

    # Reset the running status once the job is finished
@app.route('/')
def index():
    return render_template('index.html')

def is_valid_file(file, data_type):
    extension = file.filename.rsplit('.', 1)[-1].lower()
    if data_type == 'structured':
        return extension in STRUCTURED_EXTENSIONS
    elif data_type == 'unstructured':
        return extension in UNSTRUCTURED_EXTENSIONS
    return False

def upload_file_to_dbfs(file, data_type):
    if data_type == 'structured':
        path = STRUCTURED_PATH
    elif data_type == 'unstructured':
        path = UNSTRUCTURED_PATH
    else:
        raise ValueError("Invalid data type")

    file_content = file.read()

    dbfs_upload_url = f"{DATABRICKS_HOST}/api/2.0/dbfs/put"
    payload = {
        "path": path + "/" + file.filename,
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
        logging.error("No file part in the request")
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files['file']
    data_type = request.form.get('data-type')

    if file.filename == '':
        logging.error("No file selected")
        return jsonify({"error": "No file selected"}), 400

    if not is_valid_file(file, data_type):
        allowed_extensions = ', '.join(STRUCTURED_EXTENSIONS) if data_type == 'structured' else ', '.join(UNSTRUCTURED_EXTENSIONS)
        error_message = f"Invalid file type for {data_type} data. Allowed types are: {allowed_extensions}"
        logging.error(error_message)
        return jsonify({"error": error_message}), 400

    try:
        upload_response = upload_file_to_dbfs(file, data_type)
        logging.info(f"File uploaded successfully to {STRUCTURED_PATH if data_type == 'structured' else UNSTRUCTURED_PATH}/{file.filename}")

        # Determine the correct job ID based on the file type
        job_id = JOB_ID_STRUCTURED if data_type == 'structured' else JOB_ID_UNSTRUCTURED

        return jsonify({
            "message": f"File uploaded successfully to {data_type} path",
            "job_id": job_id,
            "data_type": data_type  # Send the data type back
        }), 200
    except Exception as e:
        logging.error(f"File upload failed: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/trigger-job', methods=['POST'])
def trigger_job():
    data = request.json
    job_id = data.get('job_id')
    query = data.get('query')
    data_type = data.get('data_type')

    job_trigger_url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    payload = {
        "job_id": job_id,
        "notebook_params": {"query": query, "data_type": data_type}
    }

    response = requests.post(job_trigger_url, headers=headers, json=payload)

    if response.status_code == 200:
        run_id = response.json().get('run_id')
        logging.info(f"Job triggered successfully. Run ID: {run_id}")
        return jsonify({"run_id": run_id}), 200
    else:
        logging.error(f"Failed to trigger job: {response.status_code}, {response.text}")
        return jsonify({"error": f"Failed to trigger job: {response.text}"}), 500

@app.route('/check-status', methods=['GET'])
def check_status():
    run_id = request.args.get('run_id')
    run_status_url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}"

    response = requests.get(run_status_url, headers=headers)

    if response.status_code == 200:
        run_status = response.json().get('state', {}).get('life_cycle_state')
        logging.info(f"Run status: {run_status}")
        return jsonify({"run_status": run_status}), 200
    else:
        logging.error(f"Failed to get run status: {response.status_code}, {response.text}")
        return jsonify({"error": f"Failed to get run status: {response.text}"}), 500
    
@app.route('/list-files/structured', methods=['GET'])
def list_files_Structured():
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
def list_files_Unstructured():
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

@app.route('/send-prompt/structure', methods=['POST'])
def chatbot_structure():
    try:
        user_data = request.json
        user_query = user_data.get('query_text')

        if not user_query:
            return jsonify({"error": "Query is required"}), 400

        # Trigger Databricks job
        # job_run_id = trigger_databricks_job1(user_query)
        job_run_id = invoke_databricks_notebook("/Workspace/Users/hackathon_ai6@centific.com/GROUP_6/DataBricks/Unstructured", user_query)
        job_details = check_job_status(job_run_id["run_id"])
        # print(job_details)

        task_run_id = job_details['tasks'][0]['run_id']

        response_text = get_task_run_output(task_run_id)

        # # Return the chatbot's response
        # print(response_text)
        return jsonify({"message": "Databricks chatbot job finished", "response": response_text}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route("/get-answer/structure", methods=["GET"])
def trigger_databricks_job1(user_query):
    url = f'{DATABRICKS_HOST}/api/2.1/jobs/run-now'
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }

    data = {
        "job_id": "775233539514762",
        "notebook_params": {
            "query_text": user_query
        }
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        job_run_id = response.json().get('run_id')
        return job_run_id
    else:
        raise Exception(f"Failed to trigger Databricks job: {response.text}")

def check_job_status(run_id):
    url = f'{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}'
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}'
    }

    while True:
        response = requests.get(url, headers=headers)
        response_data = response.json()
        state = response_data['state']['life_cycle_state']

        if state == 'TERMINATED':
            if response_data['state']['result_state'] == 'SUCCESS':
                return response_data  # Return the entire response to extract task run_id
            else:
                raise Exception("Databricks job failed!")
        elif state in ['INTERNAL_ERROR', 'SKIPPED', 'FAILED']:
            raise Exception("Databricks job encountered an error!")

        time.sleep(5)
    
def get_task_run_output(run_id):
    # Use the run_id from the task to get the actual output
    url = f'{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output?run_id={run_id}'
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}'
    }

    response = requests.get(url, headers=headers)
    output_data = response.json()

    if 'notebook_output' in output_data:
        return output_data['notebook_output']['result']
    else:
        print("nooutputfound")
        raise Exception("No notebook output found.")

@app.route('/send-prompt/unstructure', methods=['POST'])
def chatbot():
    try:
        user_data = request.json
        user_query = user_data.get('query_text')

        if not user_query:
            return jsonify({"error": "Query is required"}), 400

        # Trigger Databricks job
        # job_run_id = trigger_databricks_job1(user_query)
        job_run_id = invoke_databricks_notebook("/Workspace/Users/hackathon_ai6@centific.com/GROUP_6/DataBricks/Unstructured_query", user_query)
        job_details = check_job_status(job_run_id["run_id"])
        # print(job_details)

        task_run_id = job_details['tasks'][0]['run_id']

        response_text = get_task_run_output(task_run_id)

        # # Return the chatbot's response
        # print(response_text)
        return jsonify({"message": "Databricks chatbot job finished", "response": response_text}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route("/get-answer/unstructure", methods=["GET"])
def trigger_databricks_job(user_query):
    url = f'{DATABRICKS_HOST}/api/2.1/jobs/run-now'
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }

    data = {
        "job_id": "775233539514762",
        "notebook_params": {
            "query_text": user_query
        }
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        job_run_id = response.json().get('run_id')
        return job_run_id
    else:
        raise Exception(f"Failed to trigger Databricks job: {response.text}")

def check_job_status(run_id):
    url = f'{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}'
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}'
    }

    while True:
        response = requests.get(url, headers=headers)
        response_data = response.json()
        state = response_data['state']['life_cycle_state']

        if state == 'TERMINATED':
            if response_data['state']['result_state'] == 'SUCCESS':
                return response_data  # Return the entire response to extract task run_id
            else:
                raise Exception("Databricks job failed!")
        elif state in ['INTERNAL_ERROR', 'SKIPPED', 'FAILED']:
            raise Exception("Databricks job encountered an error!")

        time.sleep(5)
    
def get_task_run_output(run_id):
    # Use the run_id from the task to get the actual output
    url = f'{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output?run_id={run_id}'
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}'
    }

    response = requests.get(url, headers=headers)
    output_data = response.json()

    if 'notebook_output' in output_data:
        return output_data['notebook_output']['result']
    else:
        print("nooutputfound")
        raise Exception("No notebook output found.")

    

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000, use_reloader=False)

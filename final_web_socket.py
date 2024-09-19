import json
import requests
import base64
import time
from flask import Flask, render_template, send_file  # Import send_file
from flask_socketio import SocketIO, emit
from datetime import datetime, timezone

# Initialize Flask and Flask-SocketIO
app = Flask(__name__)
socketio = SocketIO(app)

# Databricks API credentials
DATABRICKS_HOST = 'https://adb-2391317195324727.7.azuredatabricks.net'  # Replace with your Databricks host
DATABRICKS_TOKEN = 'dapic5246a60410c2a1e1a459e987b516f87-3'  # Replace with your Databricks token
CLUSTER_ID = '0913-052228-mn4cbjcl'  # Replace with your cluster ID

# Load the JSON file containing the workflow data
with open('final_data.json') as f:
    data = json.load(f)

# Define headers for Databricks API
headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

# Helper function to get task key from task name
def get_task_key(task_name):
    return task_name.replace(" ", "_").lower()

# Helper function to upload Python file as a notebook in Databricks
def upload_python_file_as_notebook(file_path, notebook_path):
    upload_url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
    
    # Read the Python file
    with open(file_path, 'r') as f:
        python_code = f.read()
    
    # Encode the Python code in base64
    encoded_python_code = base64.b64encode(python_code.encode('utf-8')).decode('utf-8')
    
    # Prepare the payload for the Databricks import API
    payload = {
        "path": notebook_path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": encoded_python_code,
        "overwrite": True
    }
    
    # Call the Databricks API to upload the file
    response = requests.post(upload_url, headers=headers, json=payload)
    
    if response.status_code == 200:
        print(f"Uploaded Python file to Databricks notebook: {notebook_path}")
    else:
        raise Exception(f"Error uploading Python file: {response.text}")

# Function to create and run Databricks job, with real-time task updates
def create_and_run_databricks_job():
    create_job_url = f"{DATABRICKS_HOST}/api/2.1/jobs/create"
    job_payload = {
        "name": data["workflow_name"],
        "tasks": [],
        "max_concurrent_runs": 1
    }
    task_key_map = {}
    job_status = {
        "jobId": None,
        "jobName": data["workflow_name"],
        "status": "RUNNING",
        "tasks": []
    }
    
    # Create tasks for the workflow
    for task in data["tasks"]:
        task_key = get_task_key(task["task_name"])
        task_key_map[task["task_id"]] = task_key
        notebook_path = f"/Workspace/Users/hackathon_ai10@centific.com/POD_K2/conf/{task_key}_notebook"
        upload_python_file_as_notebook(task["file"], notebook_path)
        
        job_task = {
            "task_key": task_key,
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": task.get("parameters", {})
            },
            "existing_cluster_id": CLUSTER_ID
        }
        if task["dependencies"]:
            job_task["depends_on"] = [{"task_key": task_key_map.get(dep_task_id)} for dep_task_id in task["dependencies"] if task_key_map.get(dep_task_id)]
        
        job_payload["tasks"].append(job_task)
        job_status["tasks"].append({
            "name": task["task_name"],
            "runId": None,
            "status": "PENDING",
            "statusHistory": [{"status": "PENDING", "timestamp": datetime.now(timezone.utc).isoformat()}]
        })

    # Create job via Databricks API
    response = requests.post(create_job_url, headers=headers, json=job_payload)
    if response.status_code == 200:
        job_id = response.json().get('job_id')
        job_status["jobId"] = job_id
        print(f"Job '{data['workflow_name']}' created successfully. Job ID: {job_id}")
    else:
        raise Exception(f"Error creating job: {response.text}")
    
    # Run the created job
    run_job_url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    run_payload = {"job_id": job_id}
    run_response = requests.post(run_job_url, headers=headers, json=run_payload)
    if run_response.status_code == 200:
        run_id = run_response.json().get('run_id')
        print(f"Job {job_id} started successfully. Run ID: {run_id}")
    else:
        raise Exception(f"Error running job: {run_response.text}")
    
    # Check the job status with real-time task updates
    check_status_url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get"
    task_statuses = {}

    while True:
        status_response = requests.get(check_status_url, headers=headers, params={"run_id": run_id})
        if status_response.status_code == 200:
            response_data = status_response.json()
            run_status = response_data.get('state').get('life_cycle_state')
            print(f"Run {run_id} status: {run_status}")
            
            tasks = response_data.get('tasks', [])
            for task in tasks:
                task_key = task.get('task_key')
                task_state = task['state']['life_cycle_state']
                task_run_id = task.get('run_id', 'N/A')
                task_name = task.get('task_name', task_key)
                
                task_status_entry = next((t for t in job_status["tasks"] if t["name"] == task_name), None)
                if task_key not in task_statuses or task_statuses[task_key] != task_state:
                    print(f"Task '{task_name}' (Run ID: {task_run_id}) status: {task_state}")
                    if task_status_entry and task_status_entry["runId"] is None:
                        task_status_entry["runId"] = task_run_id
                    if task_status_entry:
                        task_status_entry["statusHistory"].append({
                            "status": task_state,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })
                    task_statuses[task_key] = task_state

                    # Send status updates to the frontend using WebSockets
                    socketio.emit('task_update', {
                        'task_name': task_name,
                        'status': task_state,
                        'run_id': task_run_id
                    })
            
            if run_status in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
                print(f"Job {job_id} completed with status: {run_status}")
                job_status["status"] = run_status
                socketio.emit('job_completed', {'job_id': job_id, 'status': run_status})
                break
        else:
            raise Exception(f"Error fetching job status: {status_response.text}")
        
        time.sleep(1)

    output_file = 'job_final_status.json'
    with open(output_file, 'w') as f:
        json.dump(job_status, f, indent=4)
    
    print(f"Final job status saved to {output_file}")

# Route for homepage
@app.route('/')
def index():
    return render_template('index.html')

# WebSocket route for handling the "run" event from frontend
@socketio.on('run_workflow')
def handle_run_workflow():
    print("Workflow run triggered from frontend")
    create_and_run_databricks_job()

# Route for downloading a file
@app.route('/download', methods=['GET'])
def download_file():
    filepath = 'path/to/your/static/file.txt'  # Replace with the path to the file you want to download
    try:
        return send_file(filepath, as_attachment=True)
    except Exception as e:
        return str(e), 404

# Entry point for the app with SocketIO
if __name__ == '__main__':
    socketio.run(app, debug=True)

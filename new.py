from flask import Flask, render_template, jsonify, request
import json
import requests
import time
import base64
import os

app = Flask(_name_)

# Load sensitive credentials from environment variables
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', 'https://adb-2391317195324727.7.azuredatabricks.net')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN', 'dapi2ba83a009e697fc9940656df705a7098-3')
CLUSTER_ID = os.getenv('CLUSTER_ID', '0913-052228-mn4cbjcl')

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

# Load the JSON file containing the workflow data
with open('sample_data.json') as f:
    data = json.load(f)

# Function to upload a notebook to Databricks
def upload_notebook(local_path, dbx_path):
    with open(local_path, 'r') as notebook_file:
        notebook_content = notebook_file.read()

    notebook_content_base64 = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
    payload = {
        "path": dbx_path,
        "content": notebook_content_base64,
        "format": "SOURCE",
        "language": "PYTHON",
        "overwrite": True
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        return dbx_path
    else:
        raise Exception(f"Failed to upload notebook: {response.status_code}, {response.text}")

# Combined function to upload notebooks, create, and run a Databricks job
def create_and_run_databricks_job():
    notebook_paths = {
        "Fetching Data": r"C:\Users\DELL\Desktop\code\fetching_data.py",
        "Cleaning Data": r"C:\Users\DELL\Desktop\code\cleaning_data.py",
        "Transforming Data": r"C:\Users\DELL\Desktop\code\transforming_data.py",
        "Runing Analysis": r"C:\Users\DELL\Desktop\code\runing_analysis.py",
        "Generateing Report": r"C:\Users\DELL\Desktop\code\generateing_report.py"
    }
    
    databricks_folder = "/Workspace/Users/hackathon_ai10@centific.com/POD_K2/conf"
    
    # Upload notebooks to Databricks
    for task_name, local_path in notebook_paths.items():
        filename = os.path.basename(local_path)
        dbx_path = f"{databricks_folder}/{filename}"
        try:
            upload_notebook(local_path, dbx_path)
            print(f"Uploaded {local_path} to {dbx_path}")
        except Exception as e:
            print(f"Error uploading {local_path}: {e}")
    
    # Create the job
    create_job_url = f"{DATABRICKS_HOST}/api/2.1/jobs/create"
    job_payload = {
        "name": data["workflow_name"],
        "tasks": [],
        "max_concurrent_runs": 1
    }
    
    task_key_map = {}
    for task in data["tasks"]:
        task_key = task["task_name"].replace(" ", "_").lower()
        task_key_map[task["task_id"]] = task_key

        job_task = {
            "task_key": task_key,
            "notebook_task": {
                "notebook_path": f"{databricks_folder}/{task_key}.py",
                "base_parameters": {}
            },
            "existing_cluster_id": CLUSTER_ID
        }
        
        if task["dependencies"]:
            job_task["depends_on"] = [{"task_key": task_key_map.get(dep_task_id)} for dep_task_id in task["dependencies"]]
        
        job_payload["tasks"].append(job_task)
    
    response = requests.post(create_job_url, headers=headers, json=job_payload)
    if response.status_code == 200:
        job_id = response.json().get('job_id')
        print(f"Job '{data['workflow_name']}' created successfully. Job ID: {job_id}")
    else:
        raise Exception(f"Error creating job: {response.text}")

    run_job_url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    run_payload = {"job_id": job_id}
    run_response = requests.post(run_job_url, headers=headers, json=run_payload)
    
    if run_response.status_code == 200:
        run_id = run_response.json().get('run_id')
        print(f"Job {job_id} started successfully. Run ID: {run_id}")
    else:
        raise Exception(f"Error running job: {run_response.text}")
    
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
                
                if task_key not in task_statuses or task_statuses[task_key] != task_state:
                    print(f"Task '{task_key}' status: {task_state}")
                    task_statuses[task_key] = task_state
            
            if run_status in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
                print(f"Job {job_id} completed with status: {run_status}")
                break
        else:
            raise Exception(f"Error fetching job status: {status_response.text}")
        
        time.sleep(1)

@app.route('/create-job', methods=['POST'])
def create_job():
    if request.method == 'GET':
        return jsonify({"message": "Send a POST request to create the job."}), 200
    try:
        create_and_run_databricks_job()
        return jsonify({"message": "Job created and run successfully!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if _name_ == '_main_':
    app.run(debug=True)
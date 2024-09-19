from flask import Flask, request, jsonify, Response
from azure.storage.blob import BlobServiceClient
from flask_cors import CORS
import requests
import json
import time

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Azure Blob Storage setup
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=trying2024;AccountKey=Pj5JY9Z35kg5TakCha7LIyUdF4Z9Jwjd328s+mW/J83ATb3Oo8UJl05AWkeJ1kxbD8eATMm1K/b9+ASt9/GAlQ==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "video"
BLOB_NAME = "video.mp4"
PDF_BLOB_NAME = "police_report.pdf"

DATABRICKS_INSTANCE = "adb-1620865038680305.5"
DATABRICKS_TOKEN = "dapi8d33f3e2fb16ae65ec29801c33cfc415-3"
DATABRICKS_JOB_ID = "520508307377312"

DATABRICKS_HOST= "https://adb-1620865038680305.5.azuredatabricks.net/"
CHATBOT_JOB_ID = "557818818060571" # Replace with actual job ID

# Create BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
@app.route("/",methods=["GET"])
def hello():
    return "how"

# @app.route("/", methods=["GET"])
# def home():
#     return "hi"

@app.route("/upload-video/", methods=["POST"])
def upload_video():
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files['file']

    try:
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
        blob_client.upload_blob(file.stream, overwrite=True)
        return jsonify({"message": f"File uploaded successfully to {BLOB_NAME} in Azure Blob Storage."})
    except Exception as e:
        return jsonify({"error": f"Failed to upload video to Azure Blob Storage: {str(e)}"}), 500



@app.route("/get-report/", methods=["GET"])
def trigger_databricks_job():
    DATABRICKS_URL = f'https://{DATABRICKS_INSTANCE}.azuredatabricks.net/api/2.1/jobs/run-now'
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}'
    }

    payload = {
        "job_id": DATABRICKS_JOB_ID
    }

    response = requests.post(DATABRICKS_URL, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        run_id = response.json().get('run_id')
        job_status = wait_for_job_completion(run_id, "databricks")

        if job_status == "SUCCESS":
            return get_report()
        else:
            return jsonify({"error": f"Databricks job failed with status: {job_status}"}), 500
    else:
        return jsonify({"error": f"Failed to trigger job. Status code: {response.status_code}, Error: {response.text}"}), 500


def wait_for_job_completion(run_id, job_type):
    """
    Polls the job run status until it completes.
    """
    if job_type == "databricks":
        JOB_STATUS_URL = f'https://{DATABRICKS_INSTANCE}.azuredatabricks.net/api/2.1/jobs/runs/get'
        headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}'}
    elif job_type == "chatbot":
        JOB_STATUS_URL = f'https://{CHATBOT_INSTANCE}/api/jobs/runs/get'
        headers = {'Authorization': f'Bearer {CHATBOT_TOKEN}'}
    else:
        return "UNKNOWN_JOB_TYPE"

    while True:
        response = requests.get(f"{JOB_STATUS_URL}?run_id={run_id}", headers=headers)

        if response.status_code == 200:
            state = response.json().get('state', {})
            life_cycle_state = state.get('life_cycle_state')
            result_state = state.get('result_state')

            if life_cycle_state == "TERMINATED":
                return result_state
            elif life_cycle_state == "INTERNAL_ERROR" or result_state == "FAILED":
                return "FAILED"

        time.sleep(10)


def get_report():
    try:
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=PDF_BLOB_NAME)
        pdf_stream = blob_client.download_blob()
        pdf_content = pdf_stream.readall()
        response = Response(pdf_content, content_type='application/pdf')
        response.headers['Content-Disposition'] = 'attachment; filename=report.pdf'
        return response
    except Exception as e:
        return jsonify({"error": f"Failed to retrieve the PDF file: {str(e)}"}), 500

def trigger_databricks_job1(user_query):
    url = f'{DATABRICKS_HOST}/api/2.1/jobs/run-now'
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }

    data = {
        "job_id": CHATBOT_JOB_ID,
        "notebook_params": {
            "query": user_query
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
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
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
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }

    response = requests.get(url, headers=headers)
    output_data = response.json()

    if 'notebook_output' in output_data:
        return output_data['notebook_output']['result']
    else:
        raise Exception("No notebook output found.")


@app.route('/chatbot', methods=['POST'])
def chatbot():
    try:
        user_data = request.json
        user_query = user_data.get('query')

        if not user_query:
            return jsonify({"error": "Query is required"}), 400

        # Trigger Databricks job
        job_run_id = trigger_databricks_job1(user_query)

        # Check the job status and get the job details (including the task run_id)
        job_details = check_job_status(job_run_id)

        # Extract the run_id of the task from the job details
        task_run_id = job_details['tasks'][0]['run_id']

        # Get the output of the task using its run_id
        response_text = get_task_run_output(task_run_id)

        # Return the chatbot's response
        print(response_text)
        return jsonify({"message": "Databricks chatbot job finished", "response": response_text}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/vector-indexing', methods=['POST'])
def vector_indexing():
    url = f'{DATABRICKS_HOST}/api/2.1/jobs/run-now'
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}'
    }

    data = {
        "job_id": "306461820416747",
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        job_run_id = response.json().get('run_id')
        return "Job"
    else:
        raise Exception(f"Failed to trigger Databricks job: {response.text}")



# Run the Flask app (adjust host and port if needed)
if __name__ == "_main_":
    app.run(host="0.0.0.0", port=8000,debug=True ,use_reloader=False)
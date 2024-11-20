import requests
import json
import time

# Camunda REST API base URL
CAMUNDA_REST_URL = 'http://localhost:8080/engine-rest'

# Worker configuration
WORKER_ID = 'mqtt-worker'
TOPIC_NAME = 'mqttListener'
MAX_TASKS = 1
POLL_INTERVAL = 1  # seconds
LOCK_DURATION = 10000  # milliseconds (time the worker holds the task)

# Function to fetch and lock external tasks
def fetch_and_lock_tasks():
    url = f'{CAMUNDA_REST_URL}/external-task/fetchAndLock'
    headers = {'Content-Type': 'application/json'}
    payload = {
        "workerId": WORKER_ID,
        "maxTasks": MAX_TASKS,
        "usePriority": True,
        "topics": [{
            "topicName": TOPIC_NAME,
            "lockDuration": LOCK_DURATION,
            "variables": []
        }]
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        tasks = response.json()
        return tasks
    else:
        print(f"Failed to fetch tasks: {response.status_code} - {response.text}")
        return []

# Function to complete the task
def complete_task(task):
    task_id = task['id']
    mqtt_message = "Processed MQTT message"

    url = f'{CAMUNDA_REST_URL}/external-task/{task_id}/complete'
    headers = {'Content-Type': 'application/json'}
    payload = {
        "workerId": WORKER_ID,
        "variables": {
            "mqttMessage": {
                "value": mqtt_message,
                "type": "String"
            }
        }
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 204:
        print(f"Task {task_id} completed successfully.")
    else:
        print(f"Failed to complete task {task_id}: {response.status_code} - {response.text}")

# Function to handle failures
def handle_failure(task, error_message):
    task_id = task['id']

    url = f'{CAMUNDA_REST_URL}/external-task/{task_id}/failure'
    headers = {'Content-Type': 'application/json'}
    payload = {
        "workerId": WORKER_ID,
        "errorMessage": error_message,
        "retries": 0,  # Set retries to 0 to stop retries
        "retryTimeout": 5000  # Time in milliseconds before retry
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 204:
        print(f"Failure reported for task {task_id}.")
    else:
        print(f"Failed to report failure for task {task_id}: {response.status_code} - {response.text}")

# Main loop to poll for tasks and process them
while True:
    print("Polling for external tasks...")

    tasks = fetch_and_lock_tasks()

    if tasks:
        for task in tasks:
            try:
                # Process the task here
                print(f"Processing task {task['id']} with topic {task['topicName']}")

                # Complete the task
                complete_task(task)

            except Exception as e:
                # Handle any errors during task processing
                print(f"Error processing task {task['id']}: {str(e)}")
                handle_failure(task, str(e))

    # Wait for the next poll interval
    time.sleep(POLL_INTERVAL)

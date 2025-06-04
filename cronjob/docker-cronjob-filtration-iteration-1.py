import docker
import time
import requests
from datetime import timedelta
from threading import Thread
import argparse
import boto3
import json

# Argument parser setup
parser = argparse.ArgumentParser(description='Run Docker container for the filtration script and monitor its activity.')
parser.add_argument('-f', '--folders', nargs='+', help='List of folder names to process', default=["filtering_script"])

# Slack webhook configuration
slack_webhook_url = 'SLACK_URI'

# Docker client setup
docker_client = docker.from_env()

# Initialize a dictionary to store execution times and timeout statuses
execution_times = {}

def send_slack_notification(message):
    """Send a message to a predefined Slack channel via webhook."""
    data = {"text": message}
    response = requests.post(slack_webhook_url, json=data)
    if response.status_code != 200:
        print(f"Error sending Slack notification: {response.text}")

def remove_existing_container(container_name):
    """Remove an existing container with the given name, if it exists."""
    try:
        container = docker_client.containers.get(container_name)
        container.stop()
        container.remove()
    except docker.errors.NotFound:
        pass

def monitor_container(container_name, script_name, folder_name):
    """Monitor a Docker container for activity based on log length, with a grace period."""
    global execution_times
    last_log_length = 0
    inactivity_periods = 0
    grace_period_passed = False
    initial_check_interval = 1 * 60  # 1 minute
    inactivity_check_interval = 1 * 100  # 1 minute

    start_time = time.time()

    while True:
        try:
            container = docker_client.containers.get(container_name)
        except docker.errors.NotFound:
            print(f"Container {container_name} not found. It may have been removed.")
            break

        if container.status in ["exited", "stopped"]:
            print(f"Container {container_name} has stopped.")
            break

        current_logs = container.logs().decode("utf-8")
        current_log_length = len(current_logs)

        if current_log_length > last_log_length:
            last_log_length = current_log_length
            inactivity_periods = 0  # Reset inactivity counter upon new log activity.
            grace_period_passed = True  # Activity detected, grace period is over.
        else:
            if not grace_period_passed and time.time() - start_time > initial_check_interval:
                grace_period_passed = True
                last_log_length = current_log_length

            if grace_period_passed:
                inactivity_periods += 1
                if inactivity_periods == 1:
                    send_slack_notification(f"No new activity detected for {script_name} in {folder_name} for over an hour.")

                if inactivity_periods >= 3:
                    send_slack_notification(f"Stopping and removing {script_name} in {folder_name} due to prolonged inactivity.")
                    container.stop()
                    container.remove()
                    if folder_name not in execution_times:
                        execution_times[folder_name] = {}
                    execution_times[folder_name][script_name] = "Timed Out"
                    break

        time.sleep(initial_check_interval if not grace_period_passed else inactivity_check_interval)


def run_script_in_container(folder_name, script_name):
    """Run a specific script in a Docker container and monitor its execution."""
    global execution_times
    container_name = f"{folder_name}_{script_name.replace('.py', '').replace('/', '_')}"
    remove_existing_container(container_name)
    start_time = time.time()
    script_path = f"{folder_name}/{script_name}"
    container = docker_client.containers.run("561513845508.dkr.ecr.us-east-2.amazonaws.com/scraping-scripts:latest", script_path,
                                             name=container_name, detach=True, mem_limit='3584m')
    send_slack_notification(f"Started {script_name} in {folder_name}.")

    container.wait()  # Wait for the container to finish running

    if folder_name not in execution_times:
        execution_times[folder_name] = {}
    if script_name not in execution_times[folder_name]:  # If not marked as timed out, record completion.
        total_runtime_seconds = int(time.time() - start_time)
        total_runtime = str(timedelta(seconds=total_runtime_seconds))
        execution_times[folder_name][script_name] = total_runtime
        send_slack_notification(f"{script_name} in {folder_name} completed in {total_runtime}.")

def publish_sns_message(topic_arn, message):
    sns = boto3.client('sns', region_name='us-east-2')
    response = sns.publish(
        TopicArn=topic_arn,
        Message=message
    )
    return response

# Generate and send the report
def generate_report():
    report_lines = []
    for folder, scripts in execution_times.items():
        line = f"{folder} ---> "
        script_times = [f"{script} ({time})" for script, time in scripts.items()]
        line += " / ".join(script_times)
        report_lines.append(line)
    return "\n".join(report_lines)

# Main execution
args = parser.parse_args()
folders = args.folders

scripts = ["main-iteration-1.py"]

for folder in folders:
    for script in scripts:
        run_script_in_container(folder, script)

report = generate_report()
send_slack_notification("All script executions completed. Execution times:\n" + report)

topic_arn = "arn:aws:sns:us-east-2:561513845508:rehani-scraping-sns"
message = '{"action": "terminate"}'
publish_sns_message(topic_arn, message)

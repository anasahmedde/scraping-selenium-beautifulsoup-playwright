#!/bin/bash

# Define Slack webhook URL
SLACK_WEBHOOK_URL="SLACK_URI"

# Log file path
LOG_FILE="/home/ec2-user/python_logs/python.log"

# Function to send a message to Slack
send_slack_notification() {
    curl -X POST -H 'Content-type: application/json' --data "{\"text\":\"$1\"}" $SLACK_WEBHOOK_URL
}

# Function to run Python scripts and calculate execution time
run_and_time() {
    local folder=$1
    local script=$2
    local screen_name="$folder-$script" # Define a unique screen name
    start_time=$(date +%s)
    send_slack_notification "Started $script on $folder"
    # Change directory and run script within a screen session
    screen -dmS $screen_name bash -c "
        > '$LOG_FILE'; # Clear the log file;
        /usr/local/bin/docker-compose down;
        /usr/local/bin/docker-compose up -d;
        cd '/home/ec2-user/rehani-scraping-scripts/$folder';
        /usr/local/bin/python3.12 $script >> '$LOG_FILE' 2>&1;
    "

    # Wait for the screen session to complete
    while screen -list | grep -q $screen_name
    do
        sleep 1 # Wait for 1 second before checking again
    done

    end_time=$(date +%s)
    execution_time=$(($end_time - $start_time))
    echo "$folder ---> $script ($(date -ud "@$execution_time" +'%H:%M:%S'))"
}

# Initialize report variable
report="All script executions completed. Execution times:\n"

# Parse arguments for folder names and run scripts
while (( "$#" )); do
    folder=$1
    report+="$(run_and_time $folder url_extractor.py)\n"
    report+="$(run_and_time $folder detail_extractor.py)\n"
    shift
done

# Send report to Slack and print to console
send_slack_notification "$report"
echo -e "$report"

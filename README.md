# 🚀 Scraping Websites with Selenium, BeautifulSoup & Playwright

This repository provides a scalable framework for scraping property listings across multiple sites using **Selenium** 🧪, **BeautifulSoup** 🥣, and **Playwright** 🎭. It supports parallel execution of two main scripts in each target folder:

1. **`url_extractor.py`** 📥 – Collects and stores property URLs.
2. **`detail_extractor.py`** 📋 – Navigates to each URL and extracts detailed information.

A Dockerfile 🐳 is included for containerized execution, ensuring consistent environments and easy deployment.

## ✨ Features

* Folder-based organization for multiple websites 📂
* Dockerized workflow with a single base image (Python 3.11 slim) 🐳
* Chromium browser packaged inside the container 🌐
* AWS SNS integration for post-run notifications 🔔
* Slack notifications for start, completion, and inactivity alerts 💬

## 🔧 Prerequisites

* Docker installed on your system 🐳
* AWS CLI configured with credentials that have access to ECR (if using ECR images) 🔑
* A valid Slack webhook URL stored in `slack_webhook_url` within the main script 🔗
* Optional: AWS SNS topic ARN for final termination messages 🛰️

## ⚙️ Setup & Build

1. **Clone the repo** 📥

   ```bash
   git clone <your-repo-url> && cd <repo-dir>
   ```

2. **Build the Docker image** 🛠️

   ```bash
   docker build -t scraping-scripts:latest .
   ```

## ▶️ Usage

### Local Execution 🖥️

Run the main orchestrator script directly (requires Python dependencies installed):

```bash
python3 main.py --folders ethiopian_properties global_remax ...
```

### Docker Execution 🐳🚀

Use the container image for a clean, repeatable run:

```bash
docker run --rm \
  -v "$PWD":/usr/src/app \
  scraping-scripts:latest \
  main.py --folders ethiopian_properties global_remax ...
```

The `ENTRYPOINT ["python3"]` in the Dockerfile ensures that any passed arguments are executed as a Python command.

## 🐳 Dockerfile Breakdown

```Dockerfile
# Base image: lightweight Python
FROM python:3.11-slim
WORKDIR /usr/src/app
COPY . .

# Install Chromium for headless browsing
RUN apt update && apt install -y chromium chromium-driver

# Upgrade pip and install Python deps
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir --ignore-installed -r requirements.txt

# ENTRYPOINT to run Python scripts directly
ENTRYPOINT ["python3"]
```

## 🔍 Script Details

* **`docker.py`** orchestrates container launches per folder and script.
* **`monitor_container`** (optional) tracks container logs and sends Slack alerts on inactivity.
* **AWS SNS** message at the end signals downstream processes (e.g., EC2 termination).

## 🌐 Environment Variables

* `slack_webhook_url` 🔗: Your Slack incoming webhook endpoint.
* `AWS_PROFILE`, `AWS_REGION`, `AWS_ACCOUNT_ID`, `ECR_REPO_NAME` 🛠️: If pulling/pushing images from ECR.
* `SNS_TOPIC_ARN` 🛰️: Topic to publish termination instructions.

## 🚀 Extending

1. Add new folders under the root directory matching target sites.
2. Place or adapt `url_extractor.py` and `detail_extractor.py` in each folder.
3. Adjust threading, timeouts, or notification logic in `docker.py` as needed.

---

🎉 Happy scraping! 🕸️

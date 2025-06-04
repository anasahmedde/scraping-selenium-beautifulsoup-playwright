# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Update the package list and install required dependencies
RUN apt update && \
    apt install -y chromium chromium-driver

RUN pip install --upgrade pip setuptools wheel

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir --ignore-installed -r requirements.txt

# Use ENTRYPOINT to specify the base command
ENTRYPOINT ["python3"]

# Use the official Python slim image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy application code into the container
COPY . /app

# Install required system libraries, including wget
RUN apt-get update && apt-get install -y \
    libgomp1 wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the frpc binary into the container
COPY frpc_linux_aarch64_v0.2 /usr/local/lib/python3.9/site-packages/gradio/frpc_linux_aarch64_v0.2
RUN chmod +x /usr/local/lib/python3.9/site-packages/gradio/frpc_linux_aarch64_v0.2

# Define build-time argument for base64 credentials
ARG GOOGLE_APPLICATION_CREDENTIALS_B64

# Decode and write the credentials to a file
RUN echo "$GOOGLE_APPLICATION_CREDENTIALS_B64" | base64 -d > /app/credentials.json

# Define build-time argument for project ID
ARG GOOGLE_CLOUD_PROJECT

# Set the environment variable for the project
ENV GOOGLE_CLOUD_PROJECT=${GOOGLE_CLOUD_PROJECT}

# Set environment variables
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json

# Expose Gradio's default port
EXPOSE 7860

# Run the application
CMD ["python", "main.py"]

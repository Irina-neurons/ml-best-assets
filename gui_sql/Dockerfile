# Use the official Python slim image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy requirements first for better Docker layer caching
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install "cloud-sql-python-connector[pg8000]"

# Copy application code into the container
COPY . /app

# Install required system libraries (if still needed)
RUN apt-get update && apt-get install -y libgomp1 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Decode and set up Google credentials
ARG GOOGLE_APPLICATION_CREDENTIALS_B64
RUN echo "$GOOGLE_APPLICATION_CREDENTIALS_B64" | base64 -d > /app/credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/credentials.json"

# Expose Gradio's default port
EXPOSE 8080

# Run the application
CMD ["python", "main.py"]

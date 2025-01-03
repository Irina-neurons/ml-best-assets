# Use the official Python slim image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy application code into the container
COPY . /app

# Install required system libraries
RUN apt-get update && apt-get install -y libgomp1 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

ARG GOOGLE_APPLICATION_CREDENTIALS_B64
RUN echo "$GOOGLE_APPLICATION_CREDENTIALS_B64" | base64 -d > /app/credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/credentials.json"

# Expose Gradio's default port
EXPOSE 8080

# Run the application
CMD ["python", "main.py"]

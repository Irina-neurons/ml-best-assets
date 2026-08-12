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

# No credentials are baked into the image. On Cloud Run, Application Default
# Credentials resolve to the runtime service account via the metadata server,
# which mints short-lived tokens and never needs interactive reauthentication.

# Expose Gradio's default port
EXPOSE 8080

# Run the application
CMD ["python", "main.py"]

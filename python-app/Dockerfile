# Use a lightweight Python base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy dependency list and install them to the image
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code from local directory to container
COPY . .

# Entry point: run the producer script
CMD ["python", "main.py"]
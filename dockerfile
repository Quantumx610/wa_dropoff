# 1. Use official lightweight Python image
FROM python:3.11-slim

# 2. Set environment variables
# PYTHONDONTWRITEBYTECODE: Prevents Python from writing pyc files to disk
# PYTHONUNBUFFERED: Prevents Python from buffering stdout and stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. Set the working directory in the container
WORKDIR /app

# 4. Copy the requirements file into the container
COPY requirements.txt /app/

# 5. Install the dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 6. Copy the rest of the application code
COPY . /app/

# 7. Expose the port the app runs on
EXPOSE 8500

# 8. Command to run the application
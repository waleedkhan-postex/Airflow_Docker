# Optional: Use this if you want to bake your dependencies into the Docker image
# This is more efficient than installing them every time the DAG runs

FROM apache/airflow:2.8.1-python3.11

# Copy requirements and install dependencies
COPY scripts/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# If you have system dependencies, add them here:
# USER root
# RUN apt-get update && apt-get install -y \
#     your-package-here \
#     && apt-get clean
# USER airflow
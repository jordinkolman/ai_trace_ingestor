FROM python:3.14-slim

WORKDIR /app

# Copy dependency definitions first to leverage Docker caching
COPY pyproject.toml README.md ./

# Install dependencies
RUN pip install --no-cache-dir .

# Copy the rest of the application code
COPY . .

# Default command (can be overridden in docker-compose.yml)
CMD ["python", "consumer.py"]

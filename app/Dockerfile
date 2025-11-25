FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
# libpq-dev is often needed for psycopg2 compilation
# postgresql-client is REQUIRED for pg_isready used in entrypoint.sh
RUN apt-get update && apt-get install -y \
    curl \
    postgresql-client \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Copy entrypoint script and make it executable
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Expose port
EXPOSE 8501

# Health check
HEALTHCHECK CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Set the entrypoint to our script
ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]

# Run Streamlit (this is passed to "$@" in the entrypoint script)
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
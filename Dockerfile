FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Copy .env.example as fallback
COPY .env.example .

# Create entrypoint script to handle .env creation
RUN echo '#!/bin/bash\n\
if [ ! -f /app/.env ]; then\n\
    echo "⚠️  .env file not found, creating from .env.example..."\n\
    cp /app/.env.example /app/.env\n\
    echo "✓ .env file created from .env.example"\n\
fi\n\
exec python3 main.py' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Run the benchmark with entrypoint
CMD ["/app/entrypoint.sh"]

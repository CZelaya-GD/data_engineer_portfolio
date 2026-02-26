# Multi-stage: ETL build -> Production API
FROM python:3.12-slim-bookworm AS etl-builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Production API stage
FROM python:3.12-slim-bookworm

WORKDIR /app
COPY --from=etl-builder /root/.local /root/.local 
ENV PATH= /root/.local/bin:$PATH

# Copy app (layer cache optimized)
COPY etl/ ./etl/
COPY data/ ./data/ 
COPY queries.py .

# Security: Non-root user 
RUN useradd -m appuser && \
    chown -R appuser:appuser /app && \
    chmod -R 755 /app/data

USER appuser

# Expose API port 
EXPOSE 5000

# Healathcheck uses our hardened  /health endpoint 
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \ 
    CMD curl -f http://localhost:5000/health || exit 1

CMD ["python", "etl/serve_hn.py"]
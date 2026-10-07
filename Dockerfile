FROM python:3.12-slim

# The service is a single standard-library Python process; no site packages.
WORKDIR /app
COPY server.py /app/server.py

ENV FTP_HOST=0.0.0.0 \
    FTP_PORT=2121 \
    FTP_PASSIVE_PORTS=21100-21120 \
    FTP_ACCEPT_TIMEOUT=10

EXPOSE 2121 21100-21120

# No shell wrapper and no extra process: Compose runs server.py directly.
ENTRYPOINT ["python3", "/app/server.py"]

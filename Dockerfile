FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir websockets requests aiohttp bech32
COPY app.py .
EXPOSE 8181
CMD ["python", "-u", "app.py"]

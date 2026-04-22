FROM python:3.11-slim

# Playwright가 --with-deps로 직접 설치하므로 wget/curl만 준비
RUN apt-get update && apt-get install -y wget curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Chromium + 모든 시스템 의존성 자동 설치
RUN playwright install chromium --with-deps

COPY . .

ENV PORT=8080
CMD gunicorn server:app --bind 0.0.0.0:$PORT --workers 1 --timeout 120

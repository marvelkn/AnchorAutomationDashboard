FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# HuggingFace Spaces requires port 7860
EXPOSE 7860

CMD ["gunicorn", "wsgi:server", "--bind", "0.0.0.0:7860", "--workers", "2", "--timeout", "120"]

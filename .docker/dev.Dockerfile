FROM python:alpine

WORKDIR /usr/share/app

CMD ["sh", "-c", "pip install --no-cache-dir -r requirements.txt && python main.py"]
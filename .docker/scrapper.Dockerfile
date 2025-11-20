FROM mcr.microsoft.com/playwright/python:v1.55.0-jammy

WORKDIR /usr/share/app

RUN apt-get update && \
    apt-get install -y cron && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /var/log && \
    touch /var/log/cron.log

COPY scrapper/requirements.txt /usr/share/app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY scrapper/ /usr/share/app/

CMD ["python3", "main.py"]
import schedule, time, subprocess

subprocess.Popen(
    ["python3", "/usr/share/app/kafka_consumer.py"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)

def run_scrapper():
    subprocess.run(["python3", "/usr/share/app/scrapper.py"])

schedule.every(10).seconds.do(run_scrapper)

while True:
    schedule.run_pending()
    time.sleep(1)
import schedule
import time
import subprocess
import sys
from datetime import datetime

print("🚀 Démarrage de main.py", flush=True)

# Lancer le consumer avec logs visibles
consumer_process = subprocess.Popen(
    ["python3", "-u", "/usr/share/app/kafka_consumer.py"],
    stdout=sys.stdout,
    stderr=sys.stderr,
    bufsize=0
)

print(f"✅ Consumer démarré (PID: {consumer_process.pid})", flush=True)

# Vérifier si le consumer est toujours vivant
def check_consumer():
    if consumer_process.poll() is not None:
        print(f"❌ ALERTE: Le consumer est mort! Code: {consumer_process.returncode}", flush=True)
        sys.exit(1)

def run_scrapper():
    now = datetime.now()
    print(f"🔄 Lancement du scrapper - {now.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    result = subprocess.run(
        ["python3", "-u", "/usr/share/app/scrapper.py"],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("✅ Scrapper terminé avec succès", flush=True)
        if result.stdout:
            print(result.stdout, flush=True)
    else:
        print(f"❌ Scrapper a échoué: {result.stderr}", flush=True)

# Vérifier le consumer toutes les 30 secondes
schedule.every(30).seconds.do(check_consumer)

# 🚀 Lancer le scrapper immédiatement au démarrage
print("🎯 Lancement initial du scrapper...", flush=True)
run_scrapper()

# 🎯 Puis UNE FOIS PAR JOUR à 00:01 (minuit)
schedule.every().day.at("00:01").do(run_scrapper)

print("⏰ Scheduler démarré - Prochaine exécution: demain à 00:01", flush=True)

while True:
    schedule.run_pending()
    time.sleep(1)
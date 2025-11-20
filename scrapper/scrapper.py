import os
import json
import time

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from kafka import KafkaProducer, errors

# Configuration via variables d'environnement
RAW_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_BOOTSTRAP_SERVERS = [server.strip() for server in RAW_BOOTSTRAP.split(",") if server.strip()]
if not KAFKA_BOOTSTRAP_SERVERS:
    KAFKA_BOOTSTRAP_SERVERS = ["kafka:29092"]
SCRAPPER_TOPIC = os.getenv("SCRAPPER_TOPIC", "economic-events")

# Connexion à Kafka avec retry
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("✅ Connecté à Kafka")
        break
    except errors.NoBrokersAvailable:
        print("Kafka non disponible, retry dans 5 secondes...")
        time.sleep(5)

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.110 Safari/537.36",
        viewport={"width": 1280, "height": 720},
        java_script_enabled=True
    )
    page = context.new_page()

    print("🌐 Accès à TradingEconomics...")
    page.goto("https://tradingeconomics.com/", wait_until="networkidle")

    # Configuration des cookies pour filtrer USA + importance haute
    context.add_cookies([
        {"name": "calendar-countries", "value": "usa", "domain": ".tradingeconomics.com", "path": "/calendar"},
        {"name": "calendar-importance", "value": "3", "domain": ".tradingeconomics.com", "path": "/calendar"},
        {"name": "calendar-range", "value": "0", "domain": ".tradingeconomics.com", "path": "/calendar"},
    ])

    print("📅 Chargement du calendrier économique...")
    page.goto("https://tradingeconomics.com/calendar", wait_until="networkidle")

    # Gestion du consentement RGPD
    try:
        consent_button = page.wait_for_selector("button:has-text('Consent')", timeout=5000)
        consent_button.click()
        print("✅ Consentement accepté")
    except PlaywrightTimeoutError:
        print("ℹ️  Pas de modale de consentement détectée")

    try:
        # Attendre que le tableau soit chargé
        page.wait_for_selector("table#calendar", timeout=30000)
        
        # Récupérer tous les thead (headers de date) et tbody (événements)
        sections = page.query_selector_all("table#calendar > thead.table-header, table#calendar > tbody")
        
        current_date = "Date non disponible"  # 🔧 Valeur par défaut
        event_count = 0
        
        print(f"📊 Parcours de {len(sections)} sections du calendrier...\n", flush=True)
        
        for section in sections:
            tag_name = section.evaluate("el => el.tagName.toLowerCase()")
            
            if tag_name == "thead":
                # C'est un header de date
                # 🔧 CORRECTION: Vérifier aussi les classes hidden
                is_hidden = section.evaluate("el => el.classList.contains('hidden-head')")
                if not is_hidden:
                    date_cell = section.query_selector("th")
                    if date_cell:
                        new_date = date_cell.inner_text().strip()
                        if new_date:  # 🔧 Vérifier que ce n'est pas vide
                            current_date = new_date
                            print(f"📅 Date: {current_date}", flush=True)
            
            elif tag_name == "tbody":
                # C'est un groupe d'événements pour la date courante
                rows = section.query_selector_all("tr")
                
                for row in rows:
                    event_link = row.query_selector("a.calendar-event")
                    if event_link:
                        # Extraction de l'heure
                        time_cell = row.query_selector("td span[class*='event-']")
                        
                        if time_cell:
                            event_time = time_cell.inner_text().strip()
                            if not event_time or event_time == "":
                                event_time = "Aucune heure prévue"
                        else:
                            event_time = "Aucune heure prévue"
                        
                        # Extraction du nom de l'événement
                        event_name = event_link.inner_text().strip()
                        
                        # Extraction de la référence (période: SEP, OCT, Q3, etc.)
                        reference = row.query_selector("span.calendar-reference")
                        reference_text = reference.inner_text().strip() if reference else ""

                        # 🔧 Construction de l'objet événement avec current_date qui est maintenant garanti
                        event = {
                            "date": current_date,  # Utilise la dernière date valide
                            "time": event_time,
                            "event_name": event_name,
                            "reference": reference_text
                        }

                        # Envoi vers Kafka
                        producer.send(SCRAPPER_TOPIC, value=event)
                        event_count += 1
                        print(f"  ✉️  [{event_count}] {event_name} | {event_time} | Ref: {reference_text}")

        producer.flush()
        print(f"\n✅ {event_count} événements envoyés vers Kafka")

    except PlaywrightTimeoutError:
        print("❌ Aucune ligne trouvée dans le tableau")
        producer.send(SCRAPPER_TOPIC, value={"message": "Aucune annonce économique aujourd'hui"})
        print("✉️  Message d'erreur envoyé vers Kafka")
    except Exception as e:
        print(f"❌ Erreur lors du scraping: {e}")
        import traceback
        traceback.print_exc()

    browser.close()
    print("🛑 Navigateur fermé")
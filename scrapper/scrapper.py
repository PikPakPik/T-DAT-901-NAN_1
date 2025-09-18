from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from kafka import KafkaProducer, errors
import json
import time

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=["kafka:29092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
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

    page.goto("https://tradingeconomics.com/", wait_until="networkidle")

    context.add_cookies([
        {"name": "calendar-countries", "value": "usa", "domain": ".tradingeconomics.com", "path": "/calendar"},
        {"name": "calendar-importance", "value": "3", "domain": ".tradingeconomics.com", "path": "/calendar"},
        {"name": "calendar-range", "value": "0", "domain": ".tradingeconomics.com", "path": "/calendar"},
    ])

    page.goto("https://tradingeconomics.com/calendar", wait_until="networkidle")

    try:
        consent_button = page.wait_for_selector("button:has-text('Consent')", timeout=5000)
        consent_button.click()
        print("Consentement accepté")
    except PlaywrightTimeoutError:
        print("Pas de modale de consentement détectée")

    try:
        tbody = page.wait_for_selector("table#calendar tbody", timeout=30000)
        rows = tbody.query_selector_all("tr")
        for row in rows:
            event_link = row.query_selector("a.calendar-event")
            if event_link:
                time_cell = row.query_selector("td span.event-0")
                event_time = time_cell.inner_text().strip() if time_cell else "N/A"
                event_name = event_link.inner_text().strip()

                event = {
                    "time": event_time,
                    "event_name": event_name
                }

                producer.send("my-topic", value=event)
                print(f"Envoyé vers Kafka: {event}")

        producer.flush()

    except PlaywrightTimeoutError:
        print("Aucune ligne trouvée dans le tableau")
        producer.send("my-topic", value="Aucune annonce économique aujourd'hui")
        print(f"Envoyé vers Kafka: 'Aucune annonce économique aujourd'hui'")

    browser.close()
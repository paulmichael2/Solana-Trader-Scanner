import requests
import csv
import time
import os
import json

API_KEY = ""
URL = "https://api.nansen.ai/api/v1/tgm/who-bought-sold"
TOKEN_FILE = "tokens.txt"
OUTPUT_FILE = "nansen_transactions.csv"

HEADERS = {
    "Content-Type": "application/json",
    "apiKey": API_KEY
}

# === READ TOKENS ===
with open(TOKEN_FILE, "r") as f:
    tokens = [line.strip() for line in f if line.strip()]

seen_tx = set()  # Unique transactions
write_header = not os.path.exists(OUTPUT_FILE)
csv_file = open(OUTPUT_FILE, "a", newline="", encoding="utf-8")
csv_writer = None

def write_rows(rows):
    global csv_writer
    if not rows:
        return
    if csv_writer is None:
        keys = rows[0].keys()
        csv_writer = csv.DictWriter(csv_file, fieldnames=keys)
        if write_header:
            csv_writer.writeheader()
    csv_writer.writerows(rows)
    csv_file.flush()

# Track last successful page per token
last_page = {}

for token in tokens:
    print(f"\nProcessing token: {token}")
    page = last_page.get(token, 1)

    while True:
        payload = {
            "chain": "solana",
            "token_address": token,
            "date": {
                "from": "2026-02-17T00:00:00Z",
                "to": "2026-03-18T23:59:59Z"
            },
            "pagination": {
                "page": page,
                "per_page": 50
            },
            "filters": {
                
                "trade_volume_usd": {
                    "min": 100,
                    "max": 100000

                    
                }
            }
        }

        try:
            response = requests.post(URL, headers=HEADERS, json=payload)

            if response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
                if response.status_code in [500, 504]:
                    print("Timeout / heavy query, skipping to next token...")
                    break  # 🔥 Skip current token instead of restarting
                break

            data = response.json()
            transactions = data.get("data", [])

            if not transactions:
                print(f"No more data for token {token}")
                break

            rows_to_write = []
            for tx in transactions:
                tx_id = tx.get("tx_hash") or tx.get("transaction_hash") or tx.get("signature")
                if tx_id and tx_id in seen_tx:
                    continue
                seen_tx.add(tx_id)
                tx["token_address"] = token
                rows_to_write.append(tx)

            write_rows(rows_to_write)
            print(f"Page {page}: {len(rows_to_write)} saved | Total saved: {len(seen_tx)}")

            last_page[token] = page  # Save last successful page
            page += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"Exception: {e} - skipping token.")
            break

csv_file.close()
print(f"\n✅ Finished. Total unique transactions saved: {len(seen_tx)}")

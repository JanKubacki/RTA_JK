from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

tx_counter = 1

def generate_transaction():
    # TWÓJ KOD
    # Zwróć słownik z polami: tx_id, user_id, amount, store, category, timestamp
    global tx_counter
    tx_id = f"TX{tx_counter:04d}"
    tx_counter += 1
    user_id = f"u{random.randint(1, 20):02d}"
    amount = random.uniform(5.0, 5000.0)
    store = random.choice(["Warszawa", "Kraków", "Gdańsk", "Wrocław"])
    category = random.choice(["elektronika", "odzież", "żywność", "książki"])
    timestamp = datetime.now().isoformat()

    return {
    "tx_id": tx_id,
    "user_id": user_id,
    "amount": amount,
    "store": store,
    "category": category,
    "timestamp": timestamp
    }
    pass

# TWÓJ KOD
# Pętla: generuj transakcję, wyślij do tematu 'transactions', wypisz, sleep 1s
while True:
    transaction = generate_transaction()
    producer.send('transactions', value=transaction)

    print(transaction)      
    time.sleep(1)

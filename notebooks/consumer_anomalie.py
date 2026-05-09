from kafka import KafkaConsumer
import json
from datetime import datetime
from collections import defaultdict


consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='grupa_anomalie'
)

user_transactions = {}

print("Uruchomiono detektor anomalii. Nasłuchiwanie na transakcje...")

for message in consumer:
    data = message.value
    user_id = data['user_id']
    
    current_time = datetime.fromisoformat(data['timestamp'])
    
    if user_id not in user_transactions:
        user_transactions[user_id] = []
    
    # Dodanie transakcji do historii użytkownika
    user_transactions[user_id].append(current_time)
    
    # Usunięcie z historii transakcji starszych niż 60 sekund od obecnej transakcji
    user_transactions[user_id] = [
        t for t in user_transactions[user_id] 
        if (current_time - t).total_seconds() <= 60
    ]
    
    transaction_count = len(user_transactions[user_id])
    
    # Warunek anomalii -> więcej niż 3 transakcje    
    if transaction_count > 3:
        print(f"ALERT ANOMALII: Użytkownik {user_id} wykonał {transaction_count} transakcje/i w ciągu ostatnich 60 sekund!")

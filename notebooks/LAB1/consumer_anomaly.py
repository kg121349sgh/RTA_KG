from kafka import KafkaConsumer
import json
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='anomaly_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

user_history = {}

print("Uruchomiono wykrywanie anomalii...")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    
    tx_time = datetime.fromisoformat(tx['timestamp'])
    
    if user_id not in user_history:
        user_history[user_id] = []
        
    user_history[user_id].append(tx_time)
    
    recent_transactions = []
    for past_time in user_history[user_id]:
        diff_seconds = (tx_time - past_time).total_seconds()
        if diff_seconds <= 60:
            recent_transactions.append(past_time)
            
    user_history[user_id] = recent_transactions
    
    if len(user_history[user_id]) > 3:
        ilosc = len(user_history[user_id])
        print(f"ALERT FRAUD: Użytkownik {user_id} zrobił {ilosc} transakcje w ostatnie 60s!")

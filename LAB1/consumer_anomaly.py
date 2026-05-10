# Praca domowa 
from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='anomaly_group_2', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_history = defaultdict(list)

print("Nasłuchuję na anomalie prędkości (> 3 transakcje w 60s)...\n")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    
    current_time = message.timestamp / 1000.0
    
    user_history[user_id] = [ts for ts in user_history[user_id] if (current_time - ts) <= 60]
    
    user_history[user_id].append(current_time)
    
    if len(user_history[user_id]) > 3:
        print(f"ANOMALIA!: Użytkownik {user_id} zrobił {len(user_history[user_id])} transakcje w ostatnie 60s!")

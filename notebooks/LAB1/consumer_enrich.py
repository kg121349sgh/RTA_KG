from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='enrichment_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Uruchomiono wzbogacanie transakcji o risk_level...")

for message in consumer:
    tx = message.value
    
    # Logika risk_level
    if tx['amount'] > 3000:
        tx['risk_level'] = 'HIGH'
    elif tx['amount'] > 1000:
        tx['risk_level'] = 'MEDIUM'
    else:
        tx['risk_level'] = 'LOW'
        
    print(f"WZBOGACONA: {tx['tx_id']} | Risk: {tx['risk_level']} | Kwota: {tx['amount']}")

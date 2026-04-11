from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='count_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Uruchomiono zliczanie per sklep...")

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']
    
    store_counts[store] += 1
    total_amount[store] = total_amount.get(store, 0) + amount
    msg_count += 1
    
    if msg_count % 10 == 0:
        print(f"\n--- RAPORT PO {msg_count} TRANSAKCJACH ---")
        print(f"{'Sklep':<12} | {'Liczba':<7} | {'Suma':<12} | {'Średnia':<10}")
        print("-" * 50)
        for s in sorted(store_counts.keys()):
            count = store_counts[s]
            suma = total_amount[s]
            srednia = suma / count
            print(f"{s:<12} | {count:<7} | {suma:<12.2f} | {srednia:<10.2f}")

from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='stats_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

counts = defaultdict(int)
sums = defaultdict(float)
mins = {}
maxs = {}
msg_count = 0

print("Uruchomiono statystyki per kategoria...")

for message in consumer:
    tx = message.value
    cat = tx['category']
    amt = tx['amount']
    
    counts[cat] += 1
    sums[cat] += amt
    
    if cat not in mins or amt < mins[cat]:
        mins[cat] = amt
    if cat not in maxs or amt > maxs[cat]:
        maxs[cat] = amt
        
    msg_count += 1
    
    if msg_count % 10 == 0:
        print(f"\n>>> STATYSTYKI KATEGORII (wiadomość {msg_count}) <<<")
        print(f"{'Kategoria':<14} | {'Liczba':<6} | {'Suma':<10} | {'Min':<8} | {'Max':<8}")
        print("-" * 60)
        for c in sorted(counts.keys()):
            print(f"{c:<14} | {counts[c]:<6} | {sums[c]:<10.2f} | {mins[c]:<8.2f} | {maxs[c]:<8.2f}")

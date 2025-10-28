import re
from datetime import datetime

messages = []
with open("logs/fix_sorted.txt") as f:
    for line in f:
        # Example: extract FIX tags
        msg = {k: v for k, v in re.findall(r'(\d+)=([^\|]+)', line)}
        if '52' in msg:
            msg['timestamp'] = datetime.strptime(msg['52'], "%Y%m%d-%H:%M:%S.%f")
        messages.append(msg)

# Pair 35=5 and 35=8 using ClOrdID (11)
pairs = []
sent = {m['11']: m for m in messages if m.get('35') == '5'}
for m in messages:
    if m.get('35') == '8' and '11' in m and m['11'] in sent:
        t1, t2 = sent[m['11']]['timestamp'], m['timestamp']
        latency_ms = (t2 - t1).total_seconds() * 1000
        pairs.append((m['11'], latency_ms))

print(f"Matched {len(pairs)} messages")
print(f"Average latency: {sum(x[1] for x in pairs)/len(pairs):.2f} ms")

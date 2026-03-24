#Reducer used for all tasks
#!/usr/bin/env python3
import sys
current_key = None
current_total = 0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        key, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        continue
    if current_key == key:
        current_total += count
    else:
        if current_key is not None:
            print(f"{current_key}\t{current_total}")
        current_key = key
        current_total = count
if current_key is not None:
    print(f"{current_key}\t{current_total}")

#reducer used for all mapper tasks
import sys

current_key = None
current_total = 0

for line in sys.stdin:
    line = line.strip()  #Remove whitespace
    if not line:  #If the line is empty skip
        continue

    try:
        key, count = line.split('\t', 1)
        count = int(count)
    except ValueError: #If the line isn't in the correct format ignore it
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

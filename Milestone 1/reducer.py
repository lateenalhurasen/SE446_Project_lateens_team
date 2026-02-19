#Reducer used for all tasks
import sys

current_key = None
current_sum = 0

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    try:
        key, value = line.split("\t", 1)
        value = int(value)
    except ValueError:
        continue  # skip malformed lines

    if current_key is None:
        current_key = key

    if key != current_key:
        print(f"{current_key}\t{current_sum}")
        current_key = key
        current_sum = 0

    current_sum += value

if current_key is not None:
    print(f"{current_key}\t{current_sum}")

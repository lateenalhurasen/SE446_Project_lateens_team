#Task 5
import sys

# Index of relevant column
# Schema: ..., Arrest(8), ...
ARREST_IDX = 8

for line in sys.stdin:
    line = line.strip()

    # Skip empty lines
    if not line:
        continue

    # Skip header row
    if line.startswith("ID,"):
        continue

    parts = line.split(',')

    # Sanity Check: Ensure line has enough columns
    if len(parts) <= ARREST_IDX:
        continue

    # Extract field
    arrest_status = parts[ARREST_IDX].strip().lower()  # 'true' or 'false'

    # Emit (Key=Arrest Status, Value=1)
    if arrest_status == 'true':
        print("True\t1")
    elif arrest_status == 'false':
        print("False\t1")

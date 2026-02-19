#Task 2
import sys

# Index of relevant column
# Schema: ..., Primary Type(5), ...
PRIMARY_TYPE_IDX = 5

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
    if len(parts) <= PRIMARY_TYPE_IDX:
        continue

    # Extract field
    crime_type = parts[PRIMARY_TYPE_IDX].strip()

    # Emit (Key=Crime Type, Value=1)
    if crime_type:
        print(f"{crime_type}\t1")

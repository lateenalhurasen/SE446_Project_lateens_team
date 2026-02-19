#Task 3
import sys

# Index of relevant column
# Schema: ..., Location Description(7), ...
LOCATION_DESC_IDX = 7

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
    if len(parts) <= LOCATION_DESC_IDX:
        continue

    # Extract field
    location = parts[LOCATION_DESC_IDX].strip()

    # Emit (Key=Location Description, Value=1)
    if location:
        print(f"{location}\t1")

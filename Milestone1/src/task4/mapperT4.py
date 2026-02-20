#Task 4
import sys

# Index of relevant column
# Schema: Date(2) format example: 09/05/2015 01:30:00 PM
DATE_IDX = 2

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
    if len(parts) <= DATE_IDX:
        continue

    # Extract field
    date_str = parts[DATE_IDX].strip()

    # Extract year from "MM/DD/YYYY ..."
    try:
        date_part = date_str.split(' ')[0]   # "MM/DD/YYYY"
        year = date_part.split('/')[2]       # "YYYY"
    except Exception:
        continue

    # Emit (Key=Year, Value=1)
    if year.isdigit() and len(year) == 4:
        print(f"{year}\t1")

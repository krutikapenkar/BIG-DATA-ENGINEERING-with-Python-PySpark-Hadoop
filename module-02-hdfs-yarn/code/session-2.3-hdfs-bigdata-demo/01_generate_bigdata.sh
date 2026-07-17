#!/bin/bash
# =============================================================================
# Session 2.3 — Step 1: Generate a "big data" transactions dataset
# Big Data Engineering Training — Module 02
# =============================================================================
# WHY THIS STEP EXISTS:
# The whole point of this session is to let students SEE a file get split
# into blocks and spread across DataNodes instead of just hearing about it.
# That only works if the file is actually big enough. A 20-row CSV (like
# session_2_1's sample_employees.csv) never gets split — it's one tiny block.
# This script generates a synthetic e-commerce transactions CSV of
# configurable size so you can dial the demo up or down.
#
# USAGE:
#   bash 01_generate_bigdata.sh                # default: 100,000 rows (~5-8MB, fast, laptop-safe)
#   ROWS=5000000 bash 01_generate_bigdata.sh    # ~ few hundred MB — do this on the real
#                                               # Ubuntu/Hadoop box for the full "big data" effect
# =============================================================================

set -euo pipefail

ROWS="${ROWS:-100000}"
OUT_DIR="$(cd "$(dirname "$0")" && pwd)/data"
OUT_FILE="$OUT_DIR/transactions.csv"

mkdir -p "$OUT_DIR"

echo "Generating $ROWS synthetic transaction rows -> $OUT_FILE"

{
  echo "transaction_id,student_id,product,category,amount,city,transaction_date"
  awk -v rows="$ROWS" 'BEGIN {
    srand(42);
    split("Laptop,Phone,Tablet,Headphones,Keyboard,Monitor,Mouse,Charger", products, ",");
    split("Electronics,Accessories,Computers", categories, ",");
    split("Mumbai,Pune,Delhi,Bangalore,Chennai,Hyderabad,Kolkata", cities, ",");
    for (i = 1; i <= rows; i++) {
      p = products[int(rand() * 8) + 1];
      c = categories[int(rand() * 3) + 1];
      city = cities[int(rand() * 7) + 1];
      amount = int(rand() * 50000) + 500;
      day = int(rand() * 28) + 1;
      month = int(rand() * 12) + 1;
      printf "TXN%08d,STU%05d,%s,%s,%d,%s,2024-%02d-%02d\n", i, int(rand()*20000), p, c, amount, city, month, day;
    }
  }'
} > "$OUT_FILE"

echo
echo "Done:"
ls -lh "$OUT_FILE"
echo
echo "This size is enough to test the scripts end-to-end quickly."
echo "For the real 'watch it split into many blocks across DataNodes' demo,"
echo "re-run on your Ubuntu Hadoop box with a much bigger ROWS value, e.g.:"
echo "  ROWS=5000000 bash 01_generate_bigdata.sh"

### The import script
### Make sure to run checktaxa first so that this doesn't stop part way when missing taxa are found
import csv
from os import path

csvFile = r''
csvDir = r''

### SCRIPT ###

with open(path.join(csvDir, csvFile), 'r', encoding="uft8", errors='ignore') as f:
  for line in csv.DictReader(f):
    print(line)
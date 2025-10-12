#!/usr/bin/python3

import sys

assert(len(sys.argv) == 2)

f = open(sys.argv[1])
lines  = []

allocs = []

while True:
        line = f.readline()
        if len(line) < 10:
                break
        if line.find('INFO') != -1:
                continue
        pos = line.find('Status')
        if pos < 0:
                pos = line.find('mace')
                if pos != 0:
                        lines.append(line[pos:])
                else:
                        lines.append(line)
        else:
                cleaned = line[pos+6:].strip().strip('{}')
                pairs = cleaned.split(',')
                tl = [tuple(pair.split(': ')) for pair in pairs]
                tl = [(k.strip(), int(v)) for k, v in tl]
                allocs.append((tl, ''.join(lines)))
                lines.clear()

# sort by alloc_size
allocs.sort(key=lambda x: x[0][1][1], reverse=True)

with open('alloc.txt', 'w') as o:
        for x in allocs:
                o.write(f'{x[0]}\n{x[1]}\n')
# sort by free_size
allocs.sort(key=lambda x: x[0][3][1], reverse=True)

with open('free.txt', 'w') as o:
        for x in allocs:
                o.write(f'{x[0]}\n{x[1]}\n')

alloc_size = 0
free_size = 0

for x in allocs:
        alloc_size += x[0][1][1]
        free_size += x[0][3][1]

print(f"total_alloc {alloc_size} total_free {free_size}")

#!/bin/bash

if [ $# -eq 0 ]; then
    echo "collect_hft.sh OUTPUT_PATH"
    echo "example: collect_hft.sh /mnt/data"
    exit 1
fi

python3 collect/hft.py $1
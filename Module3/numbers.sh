#!/bin/bash
# Script to write 1,000,000 random numbers to a file named file1.txt
rm -f file1.txt  
> file1.txt
# Loop 1,000,000 times
SECONDS=0
for i in {1..1000000}
do
    echo $RANDOM >> file1.txt
done
duration=$SECONDS
echo "Time taken to write 1,000,000 random numbers: $duration seconds"

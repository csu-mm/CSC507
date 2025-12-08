#!/bin/bash
# Script to read 1,000,000 random numbers from file1.txt then
#   double each number and Save these numbers in newfile1.txt.
# This is implemented as function and the input file name is passed during the function call.
# Function call: double_numbers_save ./file1.txt
double_numbers_save()
{
    local file="$1"                      # Input file.
    local readStartLineIndex=1           # Start reading data at this line from the input file.
    local singleReadLinesCount=1000      # Read these many lines from the input text file in 'one read'.
    local readBuffer                     # Buffer to hold data from 'Single read'.
    local maxReadCount=$(wc -l < $file)  # Input file lines count.
    maxReadCount=$((maxReadCount / singleReadLinesCount)) # This many times file reads are required.
    echo "Input file Read I/O Count: $((maxReadCount)), each I/O reads $((singleReadLinesCount)) Lines from the input file."
    local arrayLines=()                  # Array to store number read from 'readBuffer'.


    while true; do
        readBuffer=$(head -n $((readStartLineIndex + singleReadLinesCount - 1)) "$file" | tail -n $singleReadLinesCount)

        # Stop if no more lines/data to read in the input file
        [ -z "$readBuffer" ] && break

        # Load readBuffer into the arrayLines
        IFS=$'\n' read -r -d '' -a arrayLines <<< "$readBuffer"$'\n'

        for i in "${!arrayLines[@]}"; do
            arrayLines[$i]=$(( arrayLines[$i] * 2 ))  # Double the old file data.
            echo "$((arrayLines[$i]))"
        done
        
        printf "%s\n" "${arrayLines[@]}" >> newfile1.txt   # Create string from all array items then Save in the new file.

        # Move to next iteration (read cycle)
        readStartLineIndex=$((readStartLineIndex+i+1))
        if [ "$readStartLineIndex" -gt $((singleReadLinesCount * maxReadCount)) ]; then
            echo "readStartLineIndex > $((singleReadLinesCount * maxReadCount))"
            break  # required number of read completed, so 'break' the loop
        fi
    done
}

rm -f newfile1.txt  # Remove any existing output file from previous run.
SECONDS=0           
double_numbers_save ./file1.txt    # Call the function
duration=$SECONDS
echo "Time taken to double each number and save for a file having 1,000,000 random numbers: $duration seconds"

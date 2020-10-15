# Information

#### Note: If experimenting with code on jupyter notebook, to prevent OOM, run with

jupyter notebook --NotbookApp.iopub_Data_Rate_Limit=1e10

## Requirements

- Python 3.6+
- Pandas

## Running of Experiments

- `python main.py` # change block_size to 100/500 in structures.py

## Running of Tests

- `python -m unittest`

## Relevant Lectures

- Lecture 03: Database Systems Storage

  - In terms of packing fields into records

    - Since our data is static, we can use fixed format with fixed length
    - Don't need record header

  - In terms of packing records into a data block

    - Unspanned method
    - Sequencing should be done for averageRating as all queries are on averageRating (we can just sort the data before inserting into db)

- Lecture 07: B+ Tree (1)

- Lecture 08: B+ Tree (2)

## Data Analysis

Analysis of data.tsv (if interested, look at data_analysis.py and run `python data_analysis.py`)

- First row in file is the columns (tconst, averageRating, numVotes)
- Second to last rows are data rows, 1 row == 1 record
- Maximum length of tconst is 10
- max(averageRating) is 10.0 while min(averageRating) is 1.0
- max(numVotes) is 2279223 while min(numVotes) is 5
- Total number of records is 1070318
- number of decimal places for averageRating < 2
- Means we can use fixed format with fixed length for record

## Record format

10 bytes for tconst (fixed string of length 10), 4 bytes for averageRating (unsigned int), 4 bytes for numVotes

Total 18 bytes for a record

## Block format

- Data Block

  - Header
    - 1 byte (for denoting if block is data/index_root/index_non_leaf/index_leaf)
    - 4 bytes (for holding block id)
    - 4 bytes (next free offset)
    - 4 bytes (for holding record size)
  - Data
    - 18n bytes (records)
    - Each data block can hold at most (block_size - 13) // 18 records

- Index Block

  - Header
    - 1 byte (for denoting if block is data/index_root/index_non_leaf/index_leaf)
    - 4 bytes (for holding block id)
    - 4 bytes (for holding parent block id)
    - 4 bytes (for holding number of keys currently in index_block)
    - 4 bytes (for holding key size)
    - Key size for our case is 14 as we are using tuple of (averageRating, tconst) as augmented key since there are duplicates for averageRating
  - Data
    - 14n bytes for keys + 8(n+1) bytes for pointers (pointer is block_id + offset)
    - Each index block can hold at most (block_size - 25) // 22 keys
      - Derivation:
        - block_size - 17 >= 14n + 8(n+1)
        - block_size - 25 >= 22n
        - (block_size - 25) / 22 >= n

## Implementation

- Disk is an array of Blocks
- Block is a bytearray
- Use little endian for numbers (4 bytes per number)
  - number.to_bytes(4, byteorder='little', signed=False)
  - number = int.from_bytes(byte_array, byteorder='little', signed=False)
  - e.g. 1000 <=> [232, 2, 0, 0]
- For strings, convert each char to ascii value, pad write with zeros (10 bytes per string)
  - e.g. "adke" <=> [115, 100, 97, 101, 0, 0, 0, 0, 0, 0]
- Represent each float with the IEEE-754 basic 32-bit binary format
- Index blocks have pointers that point to index blocks. All index block pointers point to other index blocks except leaf index blocks which point to data blocks
- Data blocks contain records

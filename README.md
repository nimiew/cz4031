# CZ4031

#### Note: If experimenting with code on jupyter notebook, to prevent OOM, run with

jupyter notebook --NotbookApp.iopub_Data_Rate_Limit=1e10

## Relevant Lectures

- Lecture 03: Database Systems Storage

  - In terms of packing fields into records

    - Since our data is static, we can use fixed format with fixed length
    - I am not sure if we need record header

  - In terms of packing records into a block

    - I suggest we go with unspanned method
    - Sequencing should be done for averageRating as all queries are on averageRating (we can just sort the data before inserting into db)

- Lecture 07: B+ Tree (1)

- Lecture 08: B+ Tree (2)

## Data

Analysis of data.tsv (if interested, look at data_analysis.py)

- First row in file is the columns (tconst, averageRating, numVotes)
- Second to last rows are data rows, 1 row == 1 record
- Maximum length of tconst is 10
- max(averageRating) is 10.0 while min(averageRating) is 1.0
- max(numVotes) is 2279223 while min(numVotes) is 5
- Total number of records is 1070318
- Means we can use fixed format with fixed length for record (10 bytes for tconst, 4 for averageRating and 4 for numVotes)

## Record format

10 bytes for tconst (fixed string of length 10), 4 bytes for averageRating (unsigned int), 4 bytes for numVotes
Total 18 bytes for a record

## Block format

- Data Block

  - Header
    - 1 byte (for denoting if block is data/index_root/index_non_leaf/index_leaf)
    - 4 bytes (for holding block id)
    - 4 bytes (for holding record length)
  - Data
    - 18n bytes (records)

- Index Block
  - Header
    - 1 byte (for denoting if block is data/index_root/index_non_leaf/index_leaf)
    - 4 bytes (for holding block id)
    - 4 bytes (for holding parent block id)
    - 4 bytes (for holding point size) (denote this as k)
    - 4 bytes (for holding key size) (denote this as p)
  - Key length is 4 as averageRating need 4 bytes
  - Pointer length is 8 as we need 4 bytes for next_block_id and 4 bytes for block offset (dense mapping!)
  - Pointer Data
    - First compute number of keys (n) that block can hold
      - block_size - 17 >= 4 \* n + 8 \* (n + 1)
      - block_size - 25 >= 12 n
      - 75 >= 12n
      - 6.25 >= n
      - n = 6
    - Hence each block will have maximum 6 keys and 7 pointers

## Implementation

- Disk is an array of Blocks
- Block is a bytearray
- Use little endian for numbers (4 bytes per number)
  - number.to_bytes(4, byteorder='little', signed=False)
  - number = int.from_bytes(byte_array, byteorder='little', signed=False)
  - e.g. 1000 <=> [232, 2, 0, 0]
- For strings, convert each char to ascii value, pad write with zeros (10 bytes per string)
  - e.g. "adke" <=> [115, 100, 97, 101, 0, 0, 0, 0, 0, 0]
- Represent each float as 2 integers (2 bytes before dot and 2 bytes after dot)
  - We can do this as I checked that before the dot got at most 2 digits and after the dot got at most 2 digits (see data_analysis.py)
  - Also all are positive
  - e.g. 5.3 <=> [5, 0, 3, 0], 11.42 <=> [11, 0, 42, 0]
- What does a pointer contain?
  - 4 bytes for block_id and 4 bytes for offset if pointing to data block
  - 4 bytes for block_id if pointing to index block

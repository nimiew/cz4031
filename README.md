# Information

#### Requirements: Python 3.6+

#### Note: If experimenting with code on jupyter notebook, to prevent OOM, run with

jupyter notebook --NotbookApp.iopub_Data_Rate_Limit=1e10

## Relevant Lectures

- Lecture 03: Database Systems Storage

  - In terms of packing fields into records

    - Since our data is static, we can use fixed format with fixed length
    - Don't need record header

  - In terms of packing records into a data block

    - I suggest we go with unspanned method
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
- number of decimal places for averageRating < 2 (so decimal places can be represent with 2 bytes (up to 65536))
- Means we can use fixed format with fixed length for record (10 bytes for tconst, 4 for averageRating and 4 for numVotes)
- Fact: There are no records who start with 0 at position 0. This means we can use value of 0 to check if there is a record at a block offset

## Record format

10 bytes for tconst (fixed string of length 10), 4 bytes for averageRating (unsigned int), 4 bytes for numVotes

Total 18 bytes for a record

## Block format

- Data Block

  - Header
    - 1 byte (for denoting if block is data/index_root/index_non_leaf/index_leaf)
    - 4 bytes (for holding block id)
    - 4 bytes (for holding record length)
    - 4 bytes (next free offset)
    - 4 bytes (next block id)
  - Data
    - 18n bytes (records)
    - Each data block can hold at most (block_size - 17) // 18 records

- Index Block

  - Header
    - 1 byte (for denoting if block is data/index_root/index_non_leaf/index_leaf)
    - 4 bytes (for holding block id)
    - 4 bytes (for holding key size)
    - 4 bytes (for holding number of keys currently in index_block)
    - Key size for our case is 4 as averageRating need 4 bytes
  - Data
    - 4n bytes (keys) + 4(n+1) bytes (pointers)
    - Each index block can hold at most (block_size - 17) // 8 keys
      - block_size - 13 >= 4n + 4(n+1)
      - block_size - 17 >= 8n
      - (block_size - 17) / 8 >= n

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
- By default, block id 0 refers to root index block
- Index blocks have pointers that point to index blocks. All index block pointers point to other index blocks except leaf index blocks which point to data blocks
- Data blocks contain records and a pointer in their header. If data block is full, its pointer will be non-zero and points to another data block. All data blocks in the same chain hold records with the same index key
- The conversion between 0 and None is the responsibility of class, not serializer/deserializer

## Tests

`python -m unittest`

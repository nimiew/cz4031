from structures import Block, Disk
from utils import *

def main():
    # read in the data as a list[list[tconst, average_rating, num_votes]]
    data = parse_data()

    # sort the records by average_rating (I think can remove this step since B+ Tree is dense index)
    data.sort(key=lambda record: record[1])

    # load next free block
    root_id = Disk.get_next_free()
    root_block = Disk.read_block(root_id)

    # get block_size for later usage
    block_size = len(root_block)

    # initialize index block 
    set_index_block_header(root_block, "root", root_id, root_id)

    # load next free block
    data_id = Disk.get_next_free()
    data_block = Disk.read_block(data_id)

    # initialize data block
    set_data_block_header(data_block, data_id)
    
    # insert the data
    search_from = 9
    for record in data:
        record_bytes = convert_record_to_bytes(record)
        # insert into data block
        next_pos = insert_record_bytes(data_block, record_bytes, search_from)
        # Check if data block is full (note if next_pos == -1, current record is NOT yet inserted)
        if next_pos == -1:
            # Since data block is full, read and initialize another one
            data_id = Disk.get_next_free()
            data_block = Disk.read_block(data_id)
            set_data_block_header(data_block, data_id)
            search_from = 9
            next_pos = insert_record_bytes(data_block, record_bytes, search_from)
        # write to disk for every record insertion
        Disk.write_block(data_id, data_block)
        search_from = next_pos
        # TODO: insert to B+ Tree
    print(f"Number of data_blocks written to {data_id}")

    # TODO: Experiments

if __name__ == "__main__":
    main()
from structures import Block, Disk
from tree import Tree
from utils import *

def main():
    # read in the data as a list[list[tconst, average_rating, num_votes]]
    data = parse_data()

    # init tree
    tree = Tree()

    # initialize data block
    data_id = Disk.get_next_free()
    data_block = Disk.read_block(data_id)
    set_data_block_header(data_block, data_id)
    
    # insert the data
    for record in data:
        record_bytes = convert_record_to_bytes(record)
        # insert into data block
        inserted_at = insert_record_bytes(data_block, record_bytes)
        if inserted_at == -1:
            data_id = Disk.get_next_free()
            data_block = Disk.read_block(data_id)
            set_data_block_header(data_block, data_id)
            inserted_at = insert_record_bytes(data_block, record_bytes)
            assert inserted_at != -1
        # write to disk for every record insertion
        Disk.write_block(data_id, data_block)
        # insert to B+ Tree (block_id, offset, key)
        tree.insert(record[1], (data_id, inserted_at))
    print(f"Number of data_blocks written to {data_id}")

    # TODO: Experiments

if __name__ == "__main__":
    main()
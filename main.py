from structures import Block, Disk
from tree import Tree
from utils import *

import time
import random

def main():
    start = time.time()

    # read in the data as a list[list[tconst, average_rating, num_votes]]
    data = parse_data()

    # init tree
    tree = Tree()

    # initialize data block
    data_id = Disk.get_next_free()
    data_block = Disk.read_block(data_id)
    set_data_block_header(data_block, data_id)
    
    # insert the data
    # random.shuffle(data)
    for i, record in enumerate(data):
        if i != 0 and i % 50000 == 0:
            print(f"{i} records inserted")
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
        # print("data block id:", data_id)
        # insert to B+ Tree (block_id, offset, key)
        tree.insert((record[1], record[0]), (data_id, inserted_at))
    print(f"Approx Number of blocks written to {data_id}")
    end = time.time()
    print(f"Seconds for insertion: {end-start}")

    start = time.time()
    tree.save()
    end = time.time()
    print(f"Seconds for saving tree to disk: {end-start}")

    # random.shuffle(data)
    # for i, record in enumerate(data):
    #     tree.delete(record[1])
    # print(tree.get_height())
    # print(tree.get_num_nodes())

    # # tree.validate()

    # TODO: Experiments
    # experiment 3
    start = time.time()
    blocks_offsets = tree.search(8.0)
    exp3 = [convert_bytes_to_record(read_record_bytes(Disk.read_block(block_id), offset)) for block_id, offset in blocks_offsets]
    end = time.time()
    print(f"Seconds for exp3: {end-start}")
    actual_exp3 = []
    for record in data:
        if record[1] == 8.0:
            actual_exp3.append(record)
    assert sorted(exp3) == sorted(actual_exp3)
    # tree.validate()
    

    # experiment 4
    start = time.time()
    blocks_offsets = tree.search_range(7.0, 9.0)
    exp4 = [convert_bytes_to_record(read_record_bytes(Disk.read_block(block_id), offset)) for block_id, offset in blocks_offsets]
    end = time.time()
    print(f"Seconds for exp4: {end-start}")
    actual_exp4 = [record for record in data if 7.0 <= record[1] <= 9.0]
    assert sorted(exp4) == sorted(actual_exp4)
    end = time.time()
    # tree.validate()

    # experiment 5
    start = time.time()
    tree.delete(7.0)
    end = time.time()
    print(f"Seconds for exp5: {end-start}")
    blocks_offsets = tree.search_range(None, None)
    records_remaining = [convert_bytes_to_record(read_record_bytes(Disk.read_block(block_id), offset)) for block_id, offset in blocks_offsets]
    actual_records_remaining = [record for record in data if record[1] != 7.0]
    assert sorted(records_remaining) == sorted(actual_records_remaining)
    # tree.validate()

if __name__ == "__main__":
    main()
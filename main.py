from structures import Block, Disk
from tree import Tree
from tracker import Tracker
from utils import *

import time
import random

def main():
    start = time.time()

    # read in the data as a list[list[tconst, average_rating, num_votes]]
    data = parse_data()
    data.sort(key=lambda record: (record[1], record[0]))

    # init tree
    tree = Tree()

    # initialize data block
    data_id = Disk.get_next_free()
    data_block = Disk.read_block(data_id)
    set_data_block_header(data_block, data_id)

    # keep track of the number of data blocks
    num_data_blocks = 0
    
    # insert the data
    # random.shuffle(data)
    for i, record in enumerate(data):
        if i != 0 and i % 50000 == 0:
            print(f"{i} records inserted")
        record_bytes = convert_record_to_bytes(record)
        # insert into data block
        inserted_at = insert_record_bytes(data_block, record_bytes)
        if inserted_at == -1:
            num_data_blocks += 1
            data_id = Disk.get_next_free()
            data_block = Disk.read_block(data_id)
            set_data_block_header(data_block, data_id)
            inserted_at = insert_record_bytes(data_block, record_bytes)
            assert inserted_at != -1
        # write to disk for every record insertion
        Disk.write_block(data_id, data_block)
        # insert to B+ Tree (block_id, offset, key)
        tree.insert((record[1], record[0]), (data_id, inserted_at))

    end = time.time()
    print(f"Seconds for insertion: {end-start}")

    start = time.time()
    tree.save()
    end = time.time()
    print(f"Seconds for saving tree to disk: {end-start}")

    # experiment 1
    print(f"Total number of data blocks: {num_data_blocks + 1}") # num_data_blocks were fully filled, last is partially filled
    print(f"Total number of index blocks: {tree.get_num_nodes()}")

    # experiment 2
    print(f"The parameter n of the B+ tree is: {tree.root.max_keys}")
    print(f"Total number of nodes in the B+ tree is: {tree.get_num_nodes()}")
    print(f"The height of the B+ tree is: {tree.get_height()}")
    print(f"The root node contents are: keys: {tree.root.keys}, pointers: {tree.root.get_child_ids()}")
    print("The child node contents are:")
    for child in tree.root.pointers:
        print(f"keys: {child.keys}, pointers: {child.get_child_ids()}")

    # experiment 3
    Tracker.reset_all()
    blocks_offsets = tree.search(8.0)
    print(f"The number of index blocks the process accesses: {len(Tracker.track_set['leaf']) + len(Tracker.track_set['non-leaf'])}")
    print("The content of the non-leaf index nodes:")
    for index_node in Tracker.track_set["non-leaf"]:
        print(f"keys: {index_node.keys}, pointers: {index_node.get_child_ids()}")
    print("The content of the leaf index nodes:")
    for index_node in Tracker.track_set["leaf"]: # might just give first 5 in report
        print(f"keys: {index_node.keys}, pointers: {index_node.get_child_ids()}")

    unique_data_block_ids = set(block_id for block_id, _ in blocks_offsets) # since pointers can point to same data block
    print(f"The number of data blocks the process accesses: {len(unique_data_block_ids)}")
    print("The content of the data blocks:")
    for data_block_id in unique_data_block_ids: # might just give first 5 in report
        print(f"Records for data block with id {data_block_id}:")
        print(read_all_records_from_data_block(Disk.read_block(data_block_id)))

    selected_records = [convert_bytes_to_record(read_record_bytes(Disk.read_block(block_id), offset)) for block_id, offset in blocks_offsets]
    print(f"The attribute 'tconst' of the records that are returned are: {[sr[0] for sr in selected_records]}")
    # the part below only for validation
    actual_records = []
    for record in data:
        if record[1] == 8.0:
            actual_records.append(record)
    assert sorted(selected_records) == sorted(actual_records)
    # tree.validate()

    # experiment 4
    Tracker.reset_all()
    blocks_offsets = tree.search_range(7.0, 9.0)
    print(f"The number of index blocks the process accesses: {len(Tracker.track_set['leaf']) + len(Tracker.track_set['non-leaf'])}")
    print("The content of the non-leaf index nodes:")
    for index_node in Tracker.track_set["non-leaf"]:
        print(f"keys: {index_node.keys}, pointers: {index_node.get_child_ids()}")
    print("The content of the leaf index nodes:")
    for index_node in Tracker.track_set["leaf"]: # might just give first 5 in report
        print(f"keys: {index_node.keys}, pointers: {index_node.get_child_ids()}")

    unique_data_block_ids = set(block_id for block_id, _ in blocks_offsets) # since pointers can point to same data block
    print(f"The number of data blocks the process accesses: {len(unique_data_block_ids)}")
    print("The content of the data blocks:")
    for data_block_id in unique_data_block_ids: # might just give first 5 in report
        print(f"Records for data block with id {data_block_id}:")
        print(read_all_records_from_data_block(Disk.read_block(data_block_id)))

    selected_records = [convert_bytes_to_record(read_record_bytes(Disk.read_block(block_id), offset)) for block_id, offset in blocks_offsets]
    print(f"The attribute 'tconst' of the records that are returned are: {[sr[0] for sr in selected_records]}")
    # the part below only for validation
    actual_records = []
    for record in data:
        if 7.0 <= record[1] <= 9.0:
            actual_records.append(record)
    assert sorted(selected_records) == sorted(actual_records)
    # tree.validate()

    # experiment 5
    Tracker.reset_all()
    tree.delete(7.0)
    print(f"The number of times that a node is deleted: {Tracker.track_counts['merge']}")
    print(f"Total number of nodes in the B+ tree is: {tree.get_num_nodes()}")
    print(f"The height of the B+ tree is: {tree.get_height()}")
    print(f"The root node contents are: keys: {tree.root.keys}, pointers: {tree.root.get_child_ids()}")
    print("The child node contents are:")
    for child in tree.root.pointers:
        print(f"keys: {child.keys}, pointers: {child.get_child_ids()}")

    # the part below only for validation
    blocks_offsets = tree.search_range(None, None)
    records_remaining = [convert_bytes_to_record(read_record_bytes(Disk.read_block(block_id), offset)) for block_id, offset in blocks_offsets]
    actual_records_remaining = [record for record in data if record[1] != 7.0]
    assert sorted(records_remaining) == sorted(actual_records_remaining)
    # tree.validate()

if __name__ == "__main__":
    main()
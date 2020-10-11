from structures import Block, Disk
from tree import Tree
from tracker import Tracker
from utils import *

import time
import random
import pandas as pd

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

    def get_ptr_key_sequence(node):
        keys_list = node.keys
        ptrs_list = node.get_child_ids()
        lastPtr = ptrs_list.pop()
        result = ""
        result += "| headers | "
        for ptrInd, ptrVal in enumerate(ptrs_list):
            result += f"{ptrVal} | "
            result += f"{keys_list[ptrInd]} | "
        result += f"{lastPtr} | \n"
        return result

    def generate_select_query_statistic(leaf_nodes_dict, non_leaf_nodes_dict, blocks_offsets_list, file_settings):
        unique_data_block_ids = set(
            block_id for block_id, _ in blocks_offsets_list)  # since pointers can point to same data block
        selected_records = [convert_bytes_to_record(read_record_bytes(Disk.read_block(block_id), offset)) for
                            block_id, offset in blocks_offsets]

        # Index Nodes
        index_file = file_settings[0]
        data_file = file_settings[1]
        result_file = file_settings[2]

        # index nodes
        ind_file = open(index_file, "w")
        ind_file.write("The content of the non-leaf index nodes:\n")
        for ind, index_node in enumerate(Tracker.track_set["non-leaf"]):
            if index_node.parent:
                ind_file.write(
                    f"{ind}. node_id = {index_node.block_id} with parent_node_id = {index_node.parent.block_id}\n")
            else:
                ind_file.write(f"Root Node's node_id = {index_node.block_id}\n")
            ind_file.write(get_ptr_key_sequence(index_node))

        ind_file.write("\nThe content of the leaf index nodes:\n")
        count = 0
        for index_node in Tracker.track_set["leaf"]:  # might just give first 5 in report
            count += 1
            ind_file.write(
                f"{count}. node_id = {index_node.block_id} with parent_block_id = {index_node.parent.block_id}\n")
            ind_file.write(get_ptr_key_sequence(index_node))
        print(f"The number of index nodes the process accessed: {len(leaf_nodes_dict) + len(non_leaf_nodes_dict)} "
              f"{len(non_leaf_nodes_dict)} Non-leaf nodes, {len(leaf_nodes_dict)} leaf nodes)")
        print(f'Content of index nodes accessed saved to "{index_file}"\n')

        # data blocks
        d_file = open(data_file, "w")
        d_file.write("The content of the data blocks:\n")
        for data_block_id in unique_data_block_ids:
            d_file.write(f"Records for data block with id {data_block_id}:\n")
            d_file.write("| ")
            d_file.write(f"{' | '.join('{:^27}'.format(str(record)) for record in read_all_records_from_data_block(Disk.read_block(data_block_id)))}")
            d_file.write(" |\n")
        print(f"The number of data blocks the process accessed: {len(unique_data_block_ids)}")
        print(f'Content of data blocks accessed saved to "{data_file}"\n')

        # tconst of movies
        tconst_records = [record[0] for record in selected_records]
        df = pd.DataFrame(tconst_records, columns=["tconst of movies"])
        df.to_csv(result_file)
        print(f"Result:")
        print(f'tconst of {len(selected_records)} movies saved to "{result_file}"\n')

    # experiment 1
    #TODO Need to include the calculation for the total size of database
    print("Experiment 1: Storing the data on the disk...\n")
    print(f"Total number of data blocks: {num_data_blocks + 1}") # num_data_blocks were fully filled, last is partially filled
    print(f"Total number of index blocks: {tree.get_num_nodes()}")

    # experiment 2
    print("Experiment 2: Building a B+ tree on the attribute 'averageRating'...\n")
    print(f"The parameter n of the B+ tree is: {tree.root.max_keys}")
    print(f"Total number of nodes in the B+ tree is: {tree.get_num_nodes()}")
    print(f"The height of the B+ tree is: {tree.get_height()}")
    print(f"The root node contents are: \n"
          f"node_id: {tree.root.block_id}")
    print(get_ptr_key_sequence(tree.root))

    print("The child node (of the root node) contents are:")
    count = 0
    for child in tree.root.pointers:
        count += 1
        print(f"{count}. node_id: {child.block_id}")
        print(get_ptr_key_sequence(child))

    # experiment 3
    print("Experiment 3: Retrieving tconst of movies with averageRating == 8...\n")
    file_settings = ["experiment_3_index_nodes.txt", "experiment_3_data_blocks.txt",
                     "experiment_3_tconst_result.csv"]

    Tracker.reset_all()
    blocks_offsets = tree.search(8.0)
    generate_select_query_statistic(Tracker.track_set['leaf'], Tracker.track_set['non-leaf'], blocks_offsets, file_settings)

    #TODO: Check with Jun Liang whether the validation below is needed

    # selected_records = [convert_bytes_to_record(read_record_bytes(Disk.read_block(block_id), offset)) for
    #                     block_id, offset in blocks_offsets]
    # # the part below only for validation
    # actual_records = []
    # for record in data:
    #     if record[1] == 8.0:
    #         actual_records.append(record)
    # assert sorted(selected_records) == sorted(actual_records)
    # # tree.validate()

    # experiment 4
    print("\nExperiment 4: Retrieving tconst of movies with 7 <= averageRating <= 9...\n")
    isSave = True
    file_settings = ["experiment_4_index_nodes.txt", "experiment_4_data_blocks.txt",
                     "experiment_4_tconst_result.csv"]
    Tracker.reset_all()
    blocks_offsets = tree.search_range(7.0, 9.0)
    generate_select_query_statistic(Tracker.track_set['leaf'], Tracker.track_set['non-leaf'], blocks_offsets, file_settings)

    #TODO: Check with Jun Liang whether the validation below is needed

    # selected_records = [convert_bytes_to_record(read_record_bytes(Disk.read_block(block_id), offset)) for block_id, offset in blocks_offsets]
    # print(f"The attribute 'tconst' of the records that are returned are: {[sr[0] for sr in selected_records]}")
    # # the part below only for validation
    # actual_records = []
    # for record in data:
    #     if 7.0 <= record[1] <= 9.0:
    #         actual_records.append(record)
    # assert sorted(selected_records) == sorted(actual_records)
    # # tree.validate()

    # experiment 5
    Tracker.reset_all()
    print("Experiment 5: Deleting movies with averageRating == 7 and Updating B+ Tree...\n")
    tree.delete(7.0)
    print(f"The number of times that a node is deleted: {Tracker.track_counts['merge']}")
    print(f"Total number of nodes in the B+ tree is: {tree.get_num_nodes()}")
    print(f"The height of the B+ tree is: {tree.get_height()}")
    print(f"The root node contents are: \n"
          f"node_id: {tree.root.block_id}")
    print(get_ptr_key_sequence(tree.root))

    print("The child node (of the root node) contents are:")
    count = 0
    for child in tree.root.pointers:
        count += 1
        print(f"{count}. node_id: {child.block_id}")
        print(get_ptr_key_sequence(child))

    # the part below only for validation
    blocks_offsets = tree.search_range(None, None)
    records_remaining = [convert_bytes_to_record(read_record_bytes(Disk.read_block(block_id), offset)) for block_id, offset in blocks_offsets]
    actual_records_remaining = [record for record in data if record[1] != 7.0]
    assert sorted(records_remaining) == sorted(actual_records_remaining)
    # tree.validate()

if __name__ == "__main__":
    main()
from structures import Block, Disk
from utils import *

def main():
    # 1. Read in the data as a list[[tconst, average_rating, num_votes]]
    data = parse_data()
    # 2. Sort the records by average_rating
    data.sort(key=lambda record: record[1])
    # 3. Get next non_full block_id
    root_id = Disk.get_next_free()
    # 4. Make it root_index block
    root_block = Disk.read_block(root_id)
    set_index_block_header(root_block, root_id, root_id, "root")
    # 5. Get next non_full block_id
    data_id = Disk.get_next_free()
    # 6. Make it data block
    data_block = Disk.read_block(data_id)
    set_data_block_header(data_block, data_id)
    # 7. Insert the data
    search_from = 9
    for record in data:
        record_bytes = convert_record_to_bytes(record)
        temp = insert_record_bytes(data_block, record_bytes, search_from)
        if temp == -1:
            # print("hi", data_id)
            print(f"Writing back block {data_id}")
            Disk.write_block(data_id, data_block)
            data_id = Disk.get_next_free()
            data_block = Disk.read_block(data_id)
            set_data_block_header(data_block, data_id)
            search_from = 9
            temp = insert_record_bytes(data_block, record_bytes, search_from)
            # print("bye", data_block)
            # print(temp)
        search_from = temp
        # print(search_from)
        # insert to index

    


if __name__ == "__main__":
    main()
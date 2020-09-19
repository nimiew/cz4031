def parse_data(path="data.tsv"):
    with open("data.tsv") as f:
        lines = f.readlines()
    data = [line.split() for i, line in enumerate(lines) if i != 0]
    for record in data:
        record[1] = float(record[1])
        record[2] = int(record[2])
    return data

def convert_string_to_bytes(string, num_bytes=10):
    # string => bytearray
    if len(string) > num_bytes:
        raise Exception(f"string length: {len(string)} exceeds num_bytes: {num_bytes}")
    res = []
    for char in string:
        byte_value = ord(char)
        if not 0 <= byte_value <= 255:
            raise Exception(f"byte value: {byte_value} invalid")
        res.append(byte_value)
    res.extend([0 for _ in range(num_bytes - len(res))])
    return bytearray(res)

def convert_bytes_to_string(bytes_):
    # bytearray => string
    res = []
    for byte in bytes_:
        if byte == 0:
            break
        res.append(chr(byte))
    return "".join(res)

def convert_float_to_bytes(real):
    # float => bytearray
    before_dot, after_dot = str(real).split(".")
    before_dot = int(before_dot).to_bytes(2, byteorder='little', signed=False)
    after_dot = int(after_dot).to_bytes(2, byteorder='little', signed=False)
    return bytearray(before_dot + after_dot) # concat

def convert_bytes_to_float(bytes_):
    # bytearray => float
    before_dot = str(int.from_bytes(bytes_[:2], byteorder='little', signed=False))
    after_dot = str(int.from_bytes(bytes_[2:], byteorder='little', signed=False))
    return float(before_dot + "." + after_dot)

def convert_uint_to_bytes(number):
    # int => bytearray
    return bytearray(number.to_bytes(4, byteorder='little', signed=False))

def convert_bytes_to_uint(bytes_):
    # bytearray => int
    return int.from_bytes(bytes_, byteorder='little', signed=False)

def convert_record_to_bytes(record):
    return convert_string_to_bytes(record[0]) + convert_float_to_bytes(record[1]) + convert_uint_to_bytes(record[2])

def convert_bytes_to_record(bytes_):
    assert(len(bytes_) == 18)
    return convert_bytes_to_string(bytes_[:10]), convert_bytes_to_float(bytes_[10:14]), convert_bytes_to_uint(bytes_[14:18])

def get_block_type(block):
    if block.bytes[0] == 0:
        return "data"
    elif block.bytes[0] == 1:
        return "root"
    elif block.bytes[0] == 2:
        return "non-leaf"
    elif block.bytes[0] == 3:
        return "leaf"
    else:
        raise Exception(f"Block type unknown! byte at position 0 is {block.bytes[0]}") 

# Bytes reserved for data block header = 9
def set_data_block_header(block, block_id, record_size=18):
    # set a data block's header based on block_id and record_size
    block.bytes[0] = 0
    block.bytes[1:5] = convert_uint_to_bytes(block_id)
    block.bytes[5:9] = convert_uint_to_bytes(record_size)
    
def get_data_block_header(block):
    # returns the block_id and record_size for a data block
    return (
        block.bytes[0],
        convert_bytes_to_uint(block.bytes[1:5]),
        convert_bytes_to_uint(block.bytes[5:9])
    )

# Bytes reserved for index block header = 17
def set_index_block_header(block, block_id, parent_block_id, index_type, pointer_size=8, key_size=4):
    # set a index block's header based on block_id, pointer_length and key_size
    if index_type == "root":
        block.bytes[0] = 1
    elif index_type == "non-leaf":
        block.bytes[0] = 2
    elif index_type == "leaf":
        block.bytes[0] = 3
    else:
        raise Exception(f"Invalid index_type: {index_type}")
    block.bytes[1:5] = convert_uint_to_bytes(block_id)
    block.bytes[5:9] = convert_uint_to_bytes(parent_block_id)
    block.bytes[9:13] = convert_uint_to_bytes(pointer_size)
    block.bytes[13:17] = convert_uint_to_bytes(key_size)
    
def get_index_block_header(block):
    # returns the index_block_type, block_id, parent_block_id, pointer_size and key_size for an index block
    return (
        block.bytes[0],
        convert_bytes_to_uint(block.bytes[1:5]),
        convert_bytes_to_uint(block.bytes[5:9]),
        convert_bytes_to_uint(block.bytes[9:13]),
        convert_bytes_to_uint(block.bytes[13:17])
    )

def insert_record(block, record_bytes, search_from=8):
    # insert the bytes of record into the first free location in data block
    # returns False if block is full and insertion is not done
    # return True if insertion is done
    if get_block_type(block) != "data":
        raise Exception("Can only insert record into data block!")
    _, block_id, record_size = get_data_block_header(block)
    if record_size != len(record_bytes):
        raise Exception(f"Error: header record size: {record_size} != len(record_bytes): {len(record_bytes)}")
    pos_to_insert = search_from
    while pos_to_insert < len(block) and block.bytes[pos_to_insert] != 0:
        pos_to_insert += record_size
    if pos_to_insert + record_size > len(block):
        return False
    block.bytes[pos_to_insert: pos_to_insert + record_size] = record_bytes
    return True
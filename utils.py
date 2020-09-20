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
        if not 1 <= byte_value <= 255:
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

def convert_float_to_bytes(float_):
    # float => bytearray
    if float_ < 1.0:
        raise Exception(f"float value must be >= 1.0")
    before_dot, after_dot = str(float_).split(".")
    before_dot, after_dot = int(before_dot), int(after_dot)
    if before_dot > 65535:
        raise Exception(f"before_dot > 65535")
    if after_dot > 65535:
        raise Exception(f"after_dot > 65535")
    before_dot = before_dot.to_bytes(2, byteorder='little', signed=False)
    after_dot = after_dot.to_bytes(2, byteorder='little', signed=False)
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
    # (string, float, int) => bytearray
    return convert_string_to_bytes(record[0]) + convert_float_to_bytes(record[1]) + convert_uint_to_bytes(record[2])

def convert_bytes_to_record(bytes_):
    # bytearray => (string, float, int)
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

# bytes reserved for data block header = 9
def set_data_block_header(block, block_id, record_size=18):
    # set a data block's header: block_id and record_size
    block.bytes[0] = 0
    block.bytes[1:5] = convert_uint_to_bytes(block_id)
    block.bytes[5:9] = convert_uint_to_bytes(record_size)
    
def get_data_block_header(block):
    # return a data block's header: block_id and record_size
    return (
        block.bytes[0],
        convert_bytes_to_uint(block.bytes[1:5]),
        convert_bytes_to_uint(block.bytes[5:9])
    )

# bytes reserved for index block header = 17
def set_index_block_header(block, index_type, block_id, parent_block_id, pointer_size=8, key_size=4):
    # set a index block's header: index_type, block_id, parent_block_id, pointer_size and key_size
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
    # return a index block's header: index_type, block_id, parent_block_id, pointer_size and key_size
    return (
        block.bytes[0],
        convert_bytes_to_uint(block.bytes[1:5]),
        convert_bytes_to_uint(block.bytes[5:9]),
        convert_bytes_to_uint(block.bytes[9:13]),
        convert_bytes_to_uint(block.bytes[13:17])
    )

def insert_record_bytes(block, record_bytes, search_from=9):
    # insert the bytes of record into the first free location in data block
    # returns -1 if block is full and insertion is not done
    # return next free location if insertion is successful
    if get_block_type(block) != "data":
        raise Exception("Can only insert record into data block!")
    _, block_id, record_size = get_data_block_header(block)
    if record_size != len(record_bytes):
        raise Exception(f"Header record size: {record_size} != len(record_bytes): {len(record_bytes)}")
    if (search_from - 9) % record_size != 0:
        raise Exception(f"search_from must satisfy {record_size}x + 9")
    pos_to_insert = search_from
    while pos_to_insert < len(block) and block.bytes[pos_to_insert] != 0:
        pos_to_insert += record_size
    if pos_to_insert + record_size > len(block):
        return -1
    block.bytes[pos_to_insert: pos_to_insert + record_size] = record_bytes
    return pos_to_insert + record_size

def read_record_bytes(block, offset):
    # return record_bytes given data block id and offset
    if get_block_type(block) != "data":
        raise Exception("Can only read record from data block!")
    _, block_id, record_size = get_data_block_header(block)
    if (offset - 9) % record_size != 0:
        raise Exception("offset must satisfy 18x + 9")
    if (offset + record_size > len(block)):
        raise Exception("offset is too big")
    return block.bytes[offset: offset + record_size]

def set_ptrs_keys_bytes(block, ptrs_keys_bytes):
    # sets the data (keys and pointers) into index block (after the header)
    if get_block_type(block) == "data":
        raise Exception("Can only set key_pointers_bytes for index block!")

    block.bytes[17:17+len(ptrs_keys_bytes)] = ptrs_keys_bytes
    # clear out the remainder
    block.bytes[17+len(ptrs_keys_bytes): len(block)] = bytearray(len(block) - (17 + len(ptrs_keys_bytes)))

def deserialize_index_block(block):
    # convert the data (keys and pointers) in a index block
    # returns list[tuple(block_id, offset), key]
    if get_block_type(block) == "data":
        raise Exception("Can only deserialize index block!")
    index_block_type, block_id, parent_block_id, pointer_size, key_size = get_index_block_header(block)
    pos = 17
    pointers = []
    keys = []
    while True:
        if pos >= len(block) or block.bytes[pos] == 0:
            break
        keys.append(convert_bytes_to_uint(block.bytes[pos:pos+key_size]))
        pos += key_size
        if pos >= len(block) or block.bytes[pos] == 0:
            break
        # recall that pointer first 4 bytes is block_id, next 4 is offset
        # offset is 0 if pointing to another index block
        # if pointing to data block, read_record_byes(block_id, offset) can be used later to retrive the record bytes
        pointer_block_id = convert_bytes_to_uint(block.bytes[pos:pos+pointer_size//2])
        pointer_offset = convert_bytes_to_uint(block.bytes[pos+pointer_size//2:pos+pointer_size])
        pointers.append((pointer_block_id, pointer_offset))
        pos += pointer_size
    return pointers, keys

def serialize_ptrs_keys(pointers, keys):
    # converts list[tuple(block_id, offset), key] into bytes, to be used with set_ptrs_keys_bytes(block, key_pointers_bytes)
    if not 0 <= len(pointers) - len(keys) <= 1:
        raise Exception(f"Invalid length of pointers and keys: len(pointers): {pointers}, len(keys): {keys}")
    res = bytearray()
    for i in range(len(keys)):
        res += convert_uint_to_bytes(keys[i]) + convert_uint_to_bytes(pointers[i][0]) + convert_uint_to_bytes(pointers[i][1])
    if len(pointers) > len(keys):
        res += convert_uint_to_bytes(pointers[i][0]) + convert_uint_to_bytes(pointers[i][1])
    return res

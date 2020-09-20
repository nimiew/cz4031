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
def set_data_block_header(block, block_id, next_free_offset=13, record_size=18):
    # set a data block's header: block_id, next_free_offset, record_size
    block.bytes[0] = 0
    block.bytes[1:5] = convert_uint_to_bytes(block_id)
    block.bytes[5:9] = convert_uint_to_bytes(next_free_offset)
    block.bytes[9:13] = convert_uint_to_bytes(record_size)
    
def get_data_block_header(block):
    # return a data block's header: block_id, next_free_offset, record_size
    return (
        block.bytes[0],
        convert_bytes_to_uint(block.bytes[1:5]),
        convert_bytes_to_uint(block.bytes[5:9]),
        convert_bytes_to_uint(block.bytes[9:13])
    )

# bytes reserved for index block header = 17
def set_index_block_header(block, index_type, block_id, parent_block_id, num_keys=0, pointer_size=8, key_size=4):
    # set a index block's header: index_type, block_id, parent_block_id, num_keys, pointer_size and key_size
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
    block.bytes[9:13] = convert_uint_to_bytes(num_keys)
    block.bytes[13:17] = convert_uint_to_bytes(pointer_size)
    block.bytes[17:21] = convert_uint_to_bytes(key_size)
    
def get_index_block_header(block):
    # return a index block's header: index_type, block_id, parent_block_id, num_keys, pointer_size and key_size
    return (
        block.bytes[0],
        convert_bytes_to_uint(block.bytes[1:5]),
        convert_bytes_to_uint(block.bytes[5:9]),
        convert_bytes_to_uint(block.bytes[9:13]),
        convert_bytes_to_uint(block.bytes[13:17]),
        convert_bytes_to_uint(block.bytes[17:21])
    )

def insert_record_bytes(block, record_bytes):
    # insert the bytes of record into the first free location in data block
    # return -1 if block is full and insertion is not done
    # return position inserted at if insertion is successful
    if get_block_type(block) != "data":
        raise Exception("Can only insert record into data block!")
    _, block_id, next_free_offset, record_size = get_data_block_header(block)
    if record_size != len(record_bytes):
        raise Exception(f"Header record size: {record_size} != len(record_bytes): {len(record_bytes)}")
    if (next_free_offset - 13) % record_size != 0:
        raise Exception(f"next_free_offset must satisfy {record_size}x + 13")
    if next_free_offset + record_size > len(block):
        return -1
    block.bytes[next_free_offset: next_free_offset + record_size] = record_bytes
    block.bytes[5:9] = convert_uint_to_bytes(next_free_offset + record_size)
    return next_free_offset

def read_record_bytes(block, offset):
    # return record_bytes given data block id and offset
    if get_block_type(block) != "data":
        raise Exception("Can only read record from data block!")
    _, block_id, _, record_size = get_data_block_header(block)
    if (offset - 13) % record_size != 0:
        raise Exception("offset must satisfy 18x + 13")
    if (offset + record_size > len(block)):
        raise Exception("offset is too big")
    return block.bytes[offset: offset + record_size]

def set_ptrs_keys_bytes(block, ptrs_keys_bytes):
    # sets the data (keys and pointers) into index block (after the header)
    # return False if ptrs_keys_bytes is too large and setting is not done
    # return True if setting is successful
    if get_block_type(block) == "data":
        raise Exception("Can only set key_pointers_bytes for index block!")
    if 21 + len(ptrs_keys_bytes) > len(block):
        return False
    block.bytes[21:21+len(ptrs_keys_bytes)] = ptrs_keys_bytes
    # clear out the remainder
    block.bytes[21+len(ptrs_keys_bytes): len(block)] = bytearray(len(block) - (21 + len(ptrs_keys_bytes)))
    num_keys = (len(ptrs_keys_bytes) - 8) // 12
    block.bytes[9:13] = convert_uint_to_bytes(num_keys)
    return True

def deserialize_index_block(block):
    # convert the data (keys and pointers) in a index block
    # returns list[tuple(block_id, offset), key]
    if get_block_type(block) == "data":
        raise Exception("Can only deserialize index block!")
    index_block_type, block_id, parent_block_id, num_keys, pointer_size, key_size = get_index_block_header(block)
    # recall that pointer first 4 bytes is block_id, next 4 is offset
    block_id_size = offset_size = pointer_size // 2
    pos = 21
    pointers = []
    keys = []
    for i in range(num_keys):
        pointer_block_id = convert_bytes_to_uint(block.bytes[pos:pos+block_id_size])
        pos += block_id_size
        pointer_offset = convert_bytes_to_uint(block.bytes[pos:pos+offset_size])
        pos += offset_size
        pointers.append((pointer_block_id, pointer_offset))
        keys.append(convert_bytes_to_uint(block.bytes[pos:pos+key_size]))
        pos += key_size
    pointer_block_id = convert_bytes_to_uint(block.bytes[pos:pos+block_id_size])
    pos += block_id_size
    pointer_offset = convert_bytes_to_uint(block.bytes[pos:pos+offset_size])
    pos += offset_size
    pointers.append((pointer_block_id, pointer_offset))
    return pointers, keys

def serialize_ptrs_keys(pointers, keys):
    # converts list[tuple(block_id, offset), key] into bytes, to be used with set_ptrs_keys_bytes(block, key_pointers_bytes)
    assert len(pointers) - len(keys) == 1
    res = bytearray()
    for i in range(len(keys)):
        res += convert_uint_to_bytes(pointers[i][0]) + convert_uint_to_bytes(pointers[i][1]) + convert_uint_to_bytes(keys[i])
    res += convert_uint_to_bytes(pointers[-1][0]) + convert_uint_to_bytes(pointers[-1][1])
    return res

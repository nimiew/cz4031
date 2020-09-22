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
    res = bytearray(before_dot + after_dot)
    assert len(res) == 4
    return res # concat

def convert_bytes_to_float(bytes_):
    # bytearray => float
    before_dot = str(int.from_bytes(bytes_[:2], byteorder='little', signed=False))
    after_dot = str(int.from_bytes(bytes_[2:], byteorder='little', signed=False))
    return float(before_dot + "." + after_dot)

def convert_uint_to_bytes(number):
    # int => bytearray
    if number == None:
        return bytearray((0).to_bytes(4, byteorder='little', signed=False))    
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
    return [convert_bytes_to_string(bytes_[:10]), convert_bytes_to_float(bytes_[10:14]), convert_bytes_to_uint(bytes_[14:18])]

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

# bytes reserved for data block header = 17
def set_data_block_header(block, block_id, next_block_id=0, next_free_offset=17, record_size=18):
    # set a data block's header: block_id, next_block_id, next_free_offset, record_size
    block.bytes[0] = 0
    block.bytes[1:5] = convert_uint_to_bytes(block_id)
    block.bytes[5:9] = convert_uint_to_bytes(next_block_id)
    block.bytes[9:13] = convert_uint_to_bytes(next_free_offset)
    block.bytes[13:17] = convert_uint_to_bytes(record_size)
    
def get_data_block_header(block):
    # return a data block's header: block_id, next_block_id, next_free_offset, record_size
    return (
        block.bytes[0],
        convert_bytes_to_uint(block.bytes[1:5]),
        convert_bytes_to_uint(block.bytes[5:9]),
        convert_bytes_to_uint(block.bytes[9:13]),
        convert_bytes_to_uint(block.bytes[13:17])
    )

# bytes reserved for index block header = 9
def set_index_block_header(block, index_type, block_id, num_keys=0, key_size=4):
    # set a index block's header: index_type, block_id, num_keys, key_size
    if index_type == "root":
        block.bytes[0] = 1
    elif index_type == "non-leaf":
        block.bytes[0] = 2
    elif index_type == "leaf":
        block.bytes[0] = 3
    else:
        raise Exception(f"Invalid index_type: {index_type}")
    block.bytes[1:5] = convert_uint_to_bytes(block_id)
    block.bytes[5:9] = convert_uint_to_bytes(num_keys)
    block.bytes[9:13] = convert_uint_to_bytes(key_size)
    
def get_index_block_header(block):
    # return a index block's header: index_type, block_id, num_keys, key_size
    return (
        block.bytes[0],
        convert_bytes_to_uint(block.bytes[1:5]),
        convert_bytes_to_uint(block.bytes[5:9]),
        convert_bytes_to_uint(block.bytes[9:13])
    )

def insert_record_bytes(block, record_bytes):
    # insert the bytes of record into the first free location in data block
    # return False if block is full and insertion is not done
    # return True if insertion is successful
    if get_block_type(block) != "data":
        raise Exception("Can only insert record into data block!")
    _, block_id, next_block_id, next_free_offset, record_size = get_data_block_header(block)
    if record_size != len(record_bytes):
        raise Exception(f"Header record size: {record_size} != len(record_bytes): {len(record_bytes)}")
    if (next_free_offset - 17) % record_size != 0:
        raise Exception(f"next_free_offset must satisfy {record_size}x + 17")
    if next_free_offset + record_size > len(block):
        return False
    # set the bytes
    block.bytes[next_free_offset: next_free_offset + record_size] = record_bytes
    # update next_free_offset
    block.bytes[9:13] = convert_uint_to_bytes(next_free_offset + record_size)
    return True

def read_record_bytes(block, offset):
    # return record_bytes given data block id and offset
    # client must ensure the data is actually there
    if get_block_type(block) != "data":
        raise Exception("Can only read record from data block!")
    _, block_id, _, _, record_size = get_data_block_header(block)
    if (offset - 17) % record_size != 0:
        raise Exception(f"offset must satisfy {record_size}x + 17")
    if (offset + record_size > len(block)):
        raise Exception("offset is too big")
    return block.bytes[offset: offset + record_size]

def set_ptrs_keys_bytes(block, ptrs_keys_bytes):
    # sets the data (keys and pointers) into index block (after the header)
    # return False if ptrs_keys_bytes is too large and setting is not done
    # return True if setting is successful
    if get_block_type(block) == "data":
        raise Exception("Can only set key_pointers_bytes for index block!")
    if 13 + len(ptrs_keys_bytes) > len(block):
        return False
    block.bytes[13:13+len(ptrs_keys_bytes)] = ptrs_keys_bytes
    # clear out the remainder
    block.bytes[13+len(ptrs_keys_bytes): len(block)] = bytearray(len(block) - (13 + len(ptrs_keys_bytes)))
    num_keys = (len(ptrs_keys_bytes) - 4) // 8
    # set the number of keys
    block.bytes[5:9] = convert_uint_to_bytes(num_keys)
    return True

def deserialize_index_block(block):
    # convert the data (keys and pointers) in a index block
    # returns list[block_id], list[key]
    if get_block_type(block) == "data":
        raise Exception("Can only deserialize index block!")
    index_type, _, num_keys, key_size = get_index_block_header(block)
    
    pos = 13
    pointers = []
    keys = []
    for i in range(num_keys):
        pointers.append(convert_bytes_to_uint(block.bytes[pos:pos+4]))
        pos += 4
        keys.append(convert_bytes_to_float(block.bytes[pos:pos+key_size]))
        pos += key_size
    pointers.append(convert_bytes_to_uint(block.bytes[pos:pos+4]))
    pos += 4
    return pointers, keys

def serialize_ptrs_keys(pointers, keys):
    # converts list[block_id] and list[key] into bytes, to be used with set_ptrs_keys_bytes(block, ptrs_keys_bytes)
    assert len(pointers) - len(keys) == 1
    res = bytearray()
    for i in range(len(keys)):
        res += convert_uint_to_bytes(pointers[i]) + convert_float_to_bytes(keys[i])
    res += convert_uint_to_bytes(pointers[-1])
    return res

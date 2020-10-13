import struct
def parse_data(path="data.tsv"):
    with open(path) as f:
        lines = f.readlines()
    data = [line.split() for i, line in enumerate(lines) if i != 0]
    for record in data:
        record[1] = float(record[1])
        record[2] = int(record[2])
    return data

def convert_string_to_bytes(string, num_bytes):
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
    #IEEE 754 binary32 format 
    res = bytearray(struct.pack("f", float_))
    assert len(res) == 4
    return res 

def convert_bytes_to_float(bytes_):
    # bytearray => float
    res = struct.unpack("f",bytes_)
    return round(res[0],1)

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
    return convert_string_to_bytes(record[0], 10) + convert_float_to_bytes(record[1]) + convert_uint_to_bytes(record[2])

def convert_bytes_to_record(bytes_):
    # bytearray => (string, float, int)
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

# bytes reserved for data block header = 13
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
        convert_bytes_to_uint(block.bytes[9:13]),
    )

# bytes reserved for index block header = 13
def set_index_block_header(block, index_type, block_id, parent_block_id, num_keys=0, key_size=14):
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
    block.bytes[13:17] = convert_uint_to_bytes(key_size)
    
def get_index_block_header(block):
    return (
        block.bytes[0],
        convert_bytes_to_uint(block.bytes[1:5]),
        convert_bytes_to_uint(block.bytes[5:9]),
        convert_bytes_to_uint(block.bytes[9:13]),
        convert_bytes_to_uint(block.bytes[13:17])
    )

def insert_record_bytes(block, record_bytes):
    # insert the bytes of record into the first free location in data block
    # return False if block is full and insertion is not done
    # return True if insertion is successful
    if get_block_type(block) != "data":
        raise Exception("Can only insert record into data block!")
    _, block_id, next_free_offset, record_size = get_data_block_header(block)
    if record_size != len(record_bytes):
        raise Exception(f"Header record size: {record_size} != len(record_bytes): {len(record_bytes)}")
    if (next_free_offset - 13) % record_size != 0:
        raise Exception(f"next_free_offset must satisfy {record_size}x + 13")
    if next_free_offset + record_size > len(block):
        return -1
    # set the bytes
    block.bytes[next_free_offset: next_free_offset + record_size] = record_bytes
    # update next_free_offset
    block.bytes[5:9] = convert_uint_to_bytes(next_free_offset + record_size)
    return next_free_offset

def read_record_bytes(block, offset):
    # return record_bytes given data block id and offset
    # client must ensure the data is actually there
    if get_block_type(block) != "data":
        raise Exception("Can only read record from data block!")
    _, block_id, _, record_size = get_data_block_header(block)
    if (offset - 13) % record_size != 0:
        raise Exception(f"offset must satisfy {record_size}x + 13")
    if (offset + record_size > len(block)):
        raise Exception("offset is too big")
    return block.bytes[offset: offset + record_size]

def delete_record_bytes(block, offset):
    # delete record_bytes at the specified offset
    # we maintain the invariant that there are no gaps in block between records
    if get_block_type(block) != "data":
        raise Exception("Can only delete record from data block!")
    _, _, _, record_size = get_data_block_header(block)
    if (offset - 13) % record_size != 0:
        raise Exception(f"offset must satisfy {record_size}x + 13")
    if (offset + record_size > len(block)):
        raise Exception("offset is too big")
    block.bytes[offset: offset+record_size] = bytearray(18)

def read_all_records_from_data_block(block):
    _, _, next_free_offset, record_size = get_data_block_header(block)
    cur_offset = 13
    records = []
    while cur_offset != next_free_offset:
        records.append(convert_bytes_to_record(read_record_bytes(block, cur_offset)))
        cur_offset += record_size
    return records

def set_ptrs_keys_bytes(block, ptrs_keys_bytes):
    # sets the data (keys and pointers) into index block (after the header)
    # return False if ptrs_keys_bytes is too large and setting is not done
    # return True if setting is successful
    if get_block_type(block) == "data":
        raise Exception("Can only set key_pointers_bytes for index block!")
    if 17 + len(ptrs_keys_bytes) > len(block):
        return False
    block.bytes[17:17+len(ptrs_keys_bytes)] = ptrs_keys_bytes
    # clear out the remainder
    block.bytes[17+len(ptrs_keys_bytes): len(block)] = bytearray(len(block) - (17 + len(ptrs_keys_bytes)))
    num_keys = (len(ptrs_keys_bytes) - 8) // 22
    # set the number of keys
    block.bytes[9:13] = convert_uint_to_bytes(num_keys)
    return True

def deserialize_index_block(block):
    # convert the data (keys and pointers) in a index block
    # returns list[block_id], list[key]
    if get_block_type(block) == "data":
        raise Exception("Can only deserialize index block!")
    index_type, _, _, num_keys, key_size = get_index_block_header(block)
    
    pos = 17
    pointers = []
    keys = []
    for i in range(num_keys):
        pointers.append((convert_bytes_to_uint(block.bytes[pos:pos+4]), convert_bytes_to_uint(block.bytes[pos+4:pos+8])))
        pos += 8
        keys.append((convert_bytes_to_float(block.bytes[pos:pos+4]), convert_bytes_to_string(block.bytes[pos+4: pos+14])))
        pos += key_size
    pointers.append((convert_bytes_to_uint(block.bytes[pos:pos+4]), convert_bytes_to_uint(block.bytes[pos+4:pos+8])))
    return pointers, keys

def serialize_ptrs_keys(pointers, keys):
    # converts list[(block_id, offset)] and list[key] into bytes, to be used with set_ptrs_keys_bytes(block, ptrs_keys_bytes)
    # recall block_id: 4 bytes, offset: 4 bytes, key: 14 bytes
    assert len(pointers) - len(keys) == 1
    res = bytearray()
    for i in range(len(keys)):
        res += convert_uint_to_bytes(pointers[i][0]) + convert_uint_to_bytes(pointers[i][1])
        res += convert_float_to_bytes(keys[i][0]) + convert_string_to_bytes(keys[i][1], 10)
    if pointers[-1] == None: # possible for the rightmost leaf node
        res += convert_uint_to_bytes(0) + convert_uint_to_bytes(0)
    else:
        res += convert_uint_to_bytes(pointers[-1][0]) + convert_uint_to_bytes(pointers[-1][1])
    return res

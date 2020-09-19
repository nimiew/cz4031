def parse_data(path="data.tsv"):
    with open("data.tsv") as f:
        lines = f.readlines()
    return [line.split() for i, line in enumerate(lines) if i != 0]

def convert_string_to_bytes(string, num_bytes):
    # string => list[int]
    # 0 <= int <= 255
    if len(string) > num_bytes:
        raise Exception(f"string length: {len(string)} exceeds num_bytes: {num_bytes}")
    res = []
    for char in string:
        byte_value = ord(char)
        if not 0 <= byte_value <= 255:
            raise Exception(f"byte value: {byte_value} invalid")
        res.append(byte_value)
    res.extend([0 for _ in range(num_bytes - len(res))])
    return res

def convert_bytes_to_string(bytes_):
    # bytearray or list[int] => string
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
    return before_dot + after_dot # concat

def convert_bytes_to_float(num_bytes):
    # bytearray or list[int] => float
    before_dot = str(int.from_bytes(num_bytes[:2], byteorder='little', signed=False))
    after_dot = str(int.from_bytes(num_bytes[2:], byteorder='little', signed=False))
    return float(before_dot + "." + after_dot)

def is_index_block(block):
    return block.bytes[0] == 1

# Bytes reserved for data block header = 8
def set_data_block_header(block, block_id, record_size):
    # set a data block's header based on block_id and record_size
    block.bytes[1:5] = convert_uint_to_bytes(block_id, 4)
    block.bytes[5:9] = convert_uint_to_bytes(record_size, 4)
    
def get_data_block_header(block):
    # returns the block_id and record_size for a data block
    return convert_bytes_to_uint(block.bytes[1:5]), convert_bytes_to_uint(block.bytes[5:9])

# Bytes reserved for index block header = 17
def set_index_block_header(block, block_id, pointer_size, key_size, index_type):
    # set a index block's header based on block_id, pointer_length and key_size
    block.bytes[0] = 1
    block.bytes[1:5] = convert_uint_to_bytes(block_id, 4)
    block.bytes[5:9] = convert_uint_to_bytes(pointer_size, 4)
    block.bytes[9:13] = convert_uint_to_bytes(key_size, 4)
    
def get_index_block_header(block):
    # returns the block_id, pointer_size and key_size for an index block
    return convert_bytes_to_uint(block.bytes[1:5]), convert_bytes_to_uint(block.bytes[5:9]), convert_bytes_to_uint(block.bytes[9:13])
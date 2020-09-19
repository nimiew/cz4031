def parse_data(path="data.tsv"):
    with open("data.tsv") as f:
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

def convert_number_to_bytes(number):
    # int => bytearray
    return bytearray(number.to_bytes(4, byteorder='little', signed=False))

def convert_bytes_to_number(bytes_):
    # bytearray => int
    return int.from_bytes(bytes_, byteorder='little', signed=False)

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
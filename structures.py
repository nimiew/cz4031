import copy # Using deepcopy, to enforce using write methods by client
from utils import *

# Constants
RECORD_SIZE = 18 # 10 + 4 + 4
BLOCK_SIZE = 100
DISK_SIZE = 100 * 1024 * 1024
NUM_BLOCKS = DISK_SIZE // BLOCK_SIZE

class Block:
    def __init__(self, block_size):
        self.bytes = bytearray(block_size)
    
    def read_bytes(self):
        return copy.deepcopy(self.bytes)
    
    def write_bytes(self, bytes_):
        self.bytes = bytes_
        
    def __repr__(self):
        return [value for value in self.bytes].__repr__()

class Disk:
    # Use this class as a static class. Don't instantiate. All methods are class methods.
    
    blocks = []
    for _ in range(NUM_BLOCKS):
        blocks.append(Block(BLOCK_SIZE))
    next_free_block_id = 0
    
    @classmethod
    def read_block(cls, address):
        if not 0 <= address < NUM_BLOCKS:
            raise Exception(f"Invalid block id. Address must be within [0, {NUM_BLOCKS-1}]")   
        return copy.deepcopy(cls.blocks[address])
    
    @classmethod
    def write_block(cls, address, block):
        if not 0 <= address < NUM_BLOCKS:
            raise Exception(f"Invalid block id. Address must be within [0, {NUM_BLOCKS-1}]")
        cls.blocks[address] = block
        
    @classmethod
    def write_free_block(cls, block):
        self.blocks[cls.next_free_block] = block
        next_free_block += 1 

    @classmethod
    def info(cls):
        return f"Disk size: {DISK_SIZE}, Block size: {BLOCK_SIZE}, No. blocks: {NUM_BLOCKS}"
        
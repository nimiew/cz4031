import copy # Using deepcopy, to enforce using write methods by client
import collections

from utils import *

# Constants
RECORD_SIZE = 18 # 10 + 4 + 4
BLOCK_SIZE = 100
DISK_SIZE = 100 * 1024 * 1024
NUM_BLOCKS = DISK_SIZE // BLOCK_SIZE

class Block:
    def __init__(self, block_size=BLOCK_SIZE):
        self.bytes = bytearray(block_size)
        self.size = block_size
    
    def __len__(self):
        return self.size

    def __repr__(self):
        return [value for value in self.bytes].__repr__()

class Disk:
    # Use this class as a static class. Don't instantiate. All methods are class methods.
    
    blocks = []
    for _ in range(NUM_BLOCKS):
        blocks.append(Block(BLOCK_SIZE))
    next_free_idx = 0
    non_full_data_idx_deque = collections.deque()

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
    def get_next_free(cls):
        cls.next_free_idx += 1
        if cls.next_free_idx == NUM_BLOCKS:
            raise Exception("Disk full")
        return cls.next_free_idx - 1

    @classmethod
    def add_non_full_data_idx_deque(cls, idx):
        cls.non_full_data_idx_deque.append(idx)

    @classmethod
    def get_non_full_data_idx(cls):
        if cls.non_full_data_idx_deque:
            return cls.non_full_data_idx_deque.popleft()
        return cls.get_next_free()

    @classmethod
    def info(cls):
        return f"Disk size: {DISK_SIZE}, Block size: {BLOCK_SIZE}, No. blocks: {NUM_BLOCKS}"

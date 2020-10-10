import collections

from utils import *

# constants (bytes)
RECORD_SIZE = 18
BLOCK_SIZE = 500
DISK_SIZE = 200 * 1024 * 1024
NUM_BLOCKS = DISK_SIZE // BLOCK_SIZE

class Block:
    def __init__(self, block_size=BLOCK_SIZE):
        self.bytes = bytearray(block_size)

    def __len__(self):
        return BLOCK_SIZE

    def __repr__(self):
        return [value for value in self.bytes].__repr__()

    def __eq__(self, other):
        return self.bytes == other.bytes

class Disk:
    # use this class as a static class. Don't instantiate. All methods are class methods.  
    blocks = []
    for _ in range(NUM_BLOCKS):
        blocks.append(Block())
    next_free_idx = 1 # 0 is never used to prevent getting mixed with None
    free_queue = collections.deque()
    non_full_data_queue = collections.deque()

    @classmethod
    def read_block(cls, idx):
        if not 1 <= idx < NUM_BLOCKS:
            raise Exception(f"Invalid block id. Address must be within [1, {NUM_BLOCKS-1}]")
        return cls.blocks[idx]

    # changes to the block that is read are actually reflected in Disk.blocks without explicitly using write_block
    # but should use write_block to simulate disk
    @classmethod
    def write_block(cls, idx, block):
        if not 1 <= idx < NUM_BLOCKS:
            raise Exception(f"Invalid block id. Address must be within [1, {NUM_BLOCKS-1}]")
        cls.blocks[idx] = block

    @classmethod
    def get_next_free(cls):
        # gets the id of the next free block (block is fully empty)
        if cls.free_queue:
            return cls.free_queue.popleft()
        else:
            cls.next_free_idx += 1
            if cls.next_free_idx == NUM_BLOCKS:
                raise Exception("Disk full")
            return cls.next_free_idx - 1

    @classmethod
    def get_non_full_data_block(cls):
        # return block id of any existing data block that is not full
        # if all allocated data blocks are full, return -1 (client should proceed to use get_next_free instead)
        if cls.non_full_data_queue:
            return cls.non_full_data_queue.popleft()
        return -1
        
    @classmethod
    def deallocate(cls, block_id):
        cls.free_queue.append(block_id)
        cls.write_block(block_id, Block())

    @classmethod
    def info(cls):
        return f"Disk size: {DISK_SIZE}, Block size: {BLOCK_SIZE}, No. blocks: {NUM_BLOCKS}"
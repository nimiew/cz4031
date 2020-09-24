import copy # using deepcopy to enforce client to use write_block
import collections

from utils import *

# constants (bytes)
RECORD_SIZE = 18
BLOCK_SIZE = 100
DISK_SIZE = 100 * 1024 * 1024
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
    next_free_idx = 0    
    free_queue = collections.deque()

    @classmethod
    def read_block(cls, idx):
        if not 0 <= idx < NUM_BLOCKS:
            raise Exception(f"Invalid block id. Address must be within [0, {NUM_BLOCKS-1}]")
        # changes to the block that is read are NOT reflected in Disk.blocks
        # client need to write the block back to the Disk
        return copy.deepcopy(cls.blocks[idx])

    @classmethod
    def write_block(cls, idx, block):
        if not 0 <= idx < NUM_BLOCKS:
            raise Exception(f"Invalid block id. Address must be within [0, {NUM_BLOCKS-1}]")
        cls.blocks[idx] = block

    @classmethod
    def get_next_free(cls):
        if cls.free_queue:
            v = cls.free_queue.popleft()
            return v
        else:
            cls.next_free_idx += 1
            if cls.next_free_idx == NUM_BLOCKS:
                raise Exception("Disk full")
            return cls.next_free_idx - 1

    @classmethod
    def deallocate(cls, block_id):
        cls.free_queue.append(block_id)
        cls.write_block(block_id, Block())

    @classmethod
    def info(cls):
        return f"Disk size: {DISK_SIZE}, Block size: {BLOCK_SIZE}, No. blocks: {NUM_BLOCKS}"
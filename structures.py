import copy # using deepcopy to enforce client to use write_block
import collections

from utils import *

# constants
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

class Disk:
    # use this class as a static class. Don't instantiate. All methods are class methods.  
    blocks = []
    for _ in range(NUM_BLOCKS):
        blocks.append(Block())
    # the idx of the next completely free block
    next_free_idx = 0
    # contains data blocks that are not full
    non_full_data_idx_deque = collections.deque()

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
        cls.next_free_idx += 1
        if cls.next_free_idx == NUM_BLOCKS + 1:
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

class BPTreeNode:
    def __init__(self, block):
        if get_block_type(block) == "data":
            raise Exception("Block type must be index and not data!")
        self.block = block
        self.index_type = None
        self.block_id = None
        self.parent_block_id = None
        self.pointer_length = None
        self.key_size = None
        self.pointers = []
        self.keys = []
        self.right_neighbor = None
        self.populate_fields()

    def populate_fields(self):
        index_type, block_id, parent_block_id, pointer_length, key_size = get_index_block_header(self.block)
        self.index_type = get_block_type(block)
        self.block_id = block_id
        self.parent_block_id = parent_block_id
        self.pointer_length = None
        self.key_size = None
        if self.index_type == None: raise Exception("index_type is None")
        if self.block_id == None: raise Exception("block_id is None")
        if self.parent_block_id == None: raise Exception("parent_block_id is None")
        if self.pointer_length == None: raise Exception("pointer_length is None")
        if self.key_size == None: raise Exception("key_size is None")
        self.max_key_size = (len(self.block) - 17 - self.pointer_length) // (self.pointer_length + self.key_size)
        pointers, keys = deserialize_index_block(self.block)
        if len(pointers) > len(keys):
            self.pointers = pointers[:-1]
            self.keys = keys
            self.right_pointer = pointers[-1]
        else:
            self.pointers = pointers
            self.keys = keys
            self.right_pointer = None

    def add(self, data_block_id, offset, key):
        if self.index_type == "child":
            for i, k in enumerate(self.keys):
                if key < k:
                    self.keys.insert(key)
                    self.pointers.insert((data_block_id, offset))
                    break
                elif i + 1 == len(self.keys):
                    self.keys.append(key)
                    self.pointers.append((data_block_id, offset))
            # split if needed
            if len(self.keys) > self.max_key_size:
                self.split()
        else:
            for i, k in enumerate(self.keys):
                if key < k:
                    child_index_node = BPTreeNode(Disk.read(self.pointers[i][0]))
                    child_index_node.add(data_block_id, offset, key)
                elif i + 1 == len(self.keys):
                    if self.right_pointer == None:
                        raise Exception("Something went wrong")
                    child_index_node = BPTreeNode(Disk.read(self.right_pointer[0]))

    def split(self):
        pass
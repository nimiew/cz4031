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

class Disk:
    # use this class as a static class. Don't instantiate. All methods are class methods.  
    blocks = []
    for _ in range(NUM_BLOCKS):
        blocks.append(Block())
    # the idx of the next completely free block
    # now got problem, if block_id is 0, then pointer will be 0 then some code will break
    next_free_idx = 1
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
    def __init__(self, block=None, parent_id=None):
        # if no block is provided, need to allocate a block (parent_id needed)
        # if block is provided, the Node simply wraps around this block by parsing its data
        if block == None:
            if parent_id == None:
                raise Exception("Cannot block and parent_id both None")
            block_id = Disk.get_next_free()
            block = Disk.read_block(block_id)
            set_index_block_header(root_block, "leaf", block_id, parent_id)
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
        self.populate_fields()

    def populate_fields(self):
        index_type, block_id, parent_block_id, num_keys, pointer_length, key_size = get_index_block_header(self.block)
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
        self.max_key_size = (len(self.block) - 21 - self.pointer_length) // (self.pointer_length + self.key_size)
        pointers, keys = deserialize_index_block(self.block)
        assert num_keys == len(keys)
        self.pointers = pointers
        self.keys = keys

    def add(self, data_block_id, offset, key):
        if self.index_type == "leaf":
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
                    child_block = Disk.read(self.pointers[i][0]) # should have been created
                    child_node = BPTreeNode(child_block) # populate the node
                    child_node.add(data_block_id, offset, key) # call add on the child node
                elif i + 1 == len(self.keys):
                    if self.pointers[-1][0] == 0:
                        raise Exception("Something went wrong")
                    child_block = Disk.read(self.pointers[-1][0]) # should have been created
                    child_node = BPTreeNode(child_block) # populate the node
                    child_node.add(data_block_id, offset, key) # call add on the child node

    def split(self):
        mid = (self.max_key_size + 1) // 2 # prefer to give left more
        left, right = BPTreeNode(self.block_id), BPTreeNode(self.block_id)
        left.keys = self.keys[:mid]
        left.pointers = self.pointers[:mid]
        right.keys = self.keys[mid:]
        right.pointers = self.pointers[mid:]
        self.keys = [right.keys[0]]
        self.pointers = [(left.block_id, 0), (right.block_id, 0)]
        self.index_type = "non_leaf"
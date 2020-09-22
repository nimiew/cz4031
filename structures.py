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

class DataNode:

    def __init__(self, block_id=None):
        if block_id == None:
            block_id = Disk.get_next_free() # allocates
            block = Disk.read_block(block_id)
            set_data_block_header(block, block_id)
            Disk.write_block(block_id, block)
        else:
            block = Disk.read_block(block_id)
        _, block_id, next_block_id, next_free_offset, record_size = get_data_block_header(block)
        assert get_block_type(block) == "data"
        self.block = block
        self.block_id = block_id
        if next_block_id == 0:
            self.next_block_id = None
        else:
            self.next_block_id = next_block_id
        self.next_free_offset = next_free_offset
        self.record_size = record_size
        self.records = self.parse_records()
        

    def parse_records(self):
        records = []
        for i in range(17, self.next_free_offset, self.record_size):
            records.append(convert_bytes_to_record(self.block.bytes[i:i+self.record_size]))
        return records

    def append(self, record):
        cur = self
        while cur.next_block_id != None:
            cur = DataNode(cur.next_block_id)
        if cur.next_free_offset + self.record_size > len(cur.block):
            cur.next_block_id = DataNode().block_id
            cur.flush_to_disk()
            cur = DataNode(cur.next_block_id)
        cur.records.append(record)
        cur.next_free_offset += self.record_size
        
        cur.flush_to_disk()

    def flush_to_disk(self):
        self.block = Block()
        set_data_block_header(self.block, self.block_id, self.next_block_id)
        for record in self.records:
            insert_record_bytes(self.block, convert_record_to_bytes(record))
        # print(f"DATA ID: {self.block_id}")
        Disk.write_block(self.block_id, self.block)

    def get_values(self):
        res = []
        cur = self
        while True:
            res.extend(cur.records)
            if cur.next_block_id == None:
                return res
            else:
                cur = DataNode(cur.next_block_id)
    
    def deallocate(self):
        cur = self
        while True:
            Disk.deallocate(cur.block_id)
            if cur.next_block_id == None:
                return
            else:
                cur = DataNode(cur.next_block_id)

class BPTree:
    
    def __init__(self, block_id=None):
        self.root = BPTreeNode(leaf=True, block_id=block_id)

    def insert(self, key, value):
        if self.root == None:
            self.root = BPTreeNode(leaf=True)
        self.root = self.root.insert(key, value)
        
    def search(self, key):
        if self.root == None:
            print("tree is empty")
            return None
        return self.root.search(key)
    
    def search_range(self, lower, upper):
        if self.root == None:
            print("tree is empty")
            return None
        return self.root.search_range(lower, upper)
    
    def delete(self, key):
        if self.root == None:
            print("tree is empty")
            return
        self.root.delete(key, None, None, None, float("inf"), float("-inf"))
        if self.root.keys == []:
            tmp = self.root
            self.root = self.root.pointers[0]
            tmp.deallocate()
            
    def validate(self):
        if not self.root:
            print("tree is empty")
            return
        self.root.validate(True)
        
    def show(self):
        # bfs
        cur = [self.root]
        while cur:
            nxt = []
            to_print = []
            for node in cur:
                if node == None:
                    print("Empty tree")
                    return
                for key in node.keys:
                    to_print.append(key)
                to_print.append("|")
                for pointer in node.pointers:
                    try:
                        nxt.append(BPTreeNode(block_id=pointer))
                    except:
                        break
            print(" ".join(str(x) for x in to_print))
            print()
            cur = nxt



class BPTreeNode:
    
    def __init__(self, leaf=True, block_id=None):
        # leaf param only matters if block_id is None
        if block_id == None:
            block_id = Disk.get_next_free() # allocates
            block = Disk.read_block(block_id)
            if leaf == False:
                set_index_block_header(block, "non-leaf", block_id)
            else:
                set_index_block_header(block, "leaf", block_id)
        else:
            block = Disk.read_block(block_id)
        pointers, keys = deserialize_index_block(block)
        if pointers[-1] == 0:
            pointers[-1] = None
        index_type, block_id, num_keys, key_size = get_index_block_header(block)
        assert get_block_type(block) != "data"
        self.block = block
        self.block_id = block_id
        self.keys = keys
        self.pointers = pointers # each element is block_id
        self.leaf = index_type == 3
        self.max_keys = (len(block) - 17) // 8
        self.min_leaf_keys = (self.max_keys + 1) // 2
        self.min_non_leaf_keys = self.max_keys // 2

    def deallocate(self):
        Disk.deallocate(self.block_id)

    def flush_to_disk(self):
        
        self.block = Block()
        if self.leaf:
            set_index_block_header(self.block, "leaf", self.block_id, len(self.keys))
        else:
            set_index_block_header(self.block, "non-leaf", self.block_id, len(self.keys))
        set_ptrs_keys_bytes(self.block, serialize_ptrs_keys(self.pointers, self.keys))
        Disk.write_block(self.block_id, self.block)
        
    def insert(self, key, value):
        """
        Inserts key-value into the tree
        If key does not exist, create key: DataNode, else append to existing DataNode
        Returns the root of the tree (may be changed if there is overflow on the node where the key is inserted)
        """
        if self.leaf:

            inserted = False
            for i in range(len(self.keys)):
                if key < self.keys[i]:
                    self.keys.insert(i, key)
                    data_node = DataNode()
                    data_node.append(value)
                    self.pointers.insert(i, data_node.block_id)
                    inserted = True
                    break
                elif key == self.keys[i]:
                    data_node = DataNode(block_id=self.pointers[i])
                    data_node.append(value)
                    inserted = True
                    break
            if not inserted:
                pos = len(self.keys)
                self.keys.insert(pos, key)
                data_node = DataNode()
                data_node.append(value)
                self.pointers.insert(pos, data_node.block_id)
            
            # check if exceeds the maximum number of keys
            if len(self.keys) > self.max_keys:
                num_left = (len(self.keys) + 1) // 2 # number of keys allocated to left side
                
                right_node = BPTreeNode(leaf=True)
                right_node.keys = self.keys[num_left:]
                right_node.pointers = self.pointers[num_left:]
                
                self.keys = self.keys[:num_left]
                self.pointers = self.pointers[:num_left]
                self.pointers.append(right_node.block_id) # pointer to the immediate right leaf node (for range queries)
                
                # above node will either be the new parent (if root is leaf), or merged with existing parent
                above_node = BPTreeNode(leaf=False)
                above_node.keys = [right_node.keys[0]]
                above_node.pointers = [self.block_id, right_node.block_id]

                self.flush_to_disk()
                right_node.flush_to_disk()
                above_node.flush_to_disk()
                
                return above_node
            self.flush_to_disk()
            # if no underflow, return self. This is used by caller to check if there was an overflow
            return self
        
        else:
            # pointers[pos] is the subtree to recursively call on
            pos = None
            for i in range(len(self.keys)):
                if key < self.keys[i]:
                    pos = i
                    break
            if pos == None:
                pos = len(self.keys)
            to_merge = BPTreeNode(block_id=self.pointers[pos]).insert(key, value)
            if to_merge.block_id == self.pointers[pos]:
                # since we know there is no overflow, we are done
                return self
            
            # to_merge is the node to be merged with self (it is actually above_node)
            self.pointers[pos] = to_merge.pointers[1]
            self.pointers.insert(pos, to_merge.pointers[0])
            self.keys.insert(pos, to_merge.keys[0])
            # print(self.block_id)
            # print(self.pointers)
            # print(self.keys)
            to_merge.deallocate()

            # non leaf can overflow as well
            # refer to slide 22 of lecture 8
            if len(self.keys) > self.max_keys:
                num_left = len(self.keys) // 2
                
                right_node = BPTreeNode(leaf=False)
                right_node.keys = self.keys[num_left+1:]
                right_node.pointers = self.pointers[num_left+1:]
                
                above_node = BPTreeNode(leaf=False)
                above_node.keys = [self.keys[num_left]]
                above_node.pointers = [self.block_id, right_node.block_id]
                
                self.keys = self.keys[:num_left]
                self.pointers = self.pointers[:num_left+1]
                
                self.flush_to_disk()
                right_node.flush_to_disk()
                above_node.flush_to_disk()
                return above_node
            
            self.flush_to_disk()
            return self
        
    def search(self, key):
        """
        Finds and returns the list of values corresponding to the key
        If not found, return None
        """
        if self.leaf:
            for i in range(len(self.keys)):
                if key == self.keys[i]:
                    return DataNode(block_id=self.pointers[i]).get_values()
            return None
        else:
            for i in range(len(self.keys)):
                if key < self.keys[i]:
                    return BPTreeNode(block_id=self.pointers[i]).search(key)
            return BPTreeNode(block_id=self.pointers[-1]).search(key)
    
    def search_first_gte(self, key):
        """
        A utility function used by search_range to return the first leaf node >= key
        If found, return the leaf node containing the key and the index of the key in the node
        If not found, i.e. key is smaller than all keys, return None
        """
        if self.leaf:
            for i in range(len(self.keys)):
                if self.keys[i] >= key:
                    return self, i
            if self.pointers[-1] == None:
                # this is true if self is the rightmost leaf node
                return None
            # if leaf node is not rightmost, we know the first key of the immediate right neightbour will satisfy condition
            # because self.pointers[-1].keys[0] >= some LB > key
            return BPTreeNode(block_id=self.pointers[-1]), 0
        else:
            # find the subtree to recursively call on
            for i in range(len(self.keys)):
                if key < self.keys[i]:
                    return BPTreeNode(block_id=self.pointers[i]).search_first_gte(key)
            return BPTreeNode(block_id=self.pointers[-1]).search_first_gte(key)
        
    def search_range(self, lower, upper):
        """
        Returns a list of all values whose keys are in the range [lower, upper] inclusive
        If lower is None, it is treated as no lower bound
        If upper is None, it is trated as no upper bound
        If both are None, return all values
        """
        if lower == None:
            lower = float("-inf")
        if upper == None:
            upper = float("inf")
        if lower > upper:
            return []
        first_gte = self.search_first_gte(lower)
        
        res = []
        if first_gte == None:
            return res
        node, pos = first_gte
        while node:
            for i in range(pos, len(node.keys)):
                if node.keys[i] > upper:
                    # current and all other leaf nodes on the road are greater than upper bound and not part of res
                    # so we can just return res
                    return res
                res.extend(DataNode(block_id=node.pointers[i]).get_values())
            # move to the immediate right neighbour
            node = BPTreeNode(block_id=node.pointers[-1])
            pos = 0
        # this return is needed if the res includes the rightmost leaf node
        return res
    
    def find_min(self):
        """
        Utility function to minimum value rooted at self
        """
        if self.leaf:
            return self.keys[0]
        return BPTreeNode(block_id=self.pointers[0]).find_min()
    
    def delete(self, key, left_neighbour, right_neighbour, parent, upper_bound, lower_bound):
        """
        different return values have different meaning
        if return [4] => key doesn't exist
        if return [0] => the function was called on a leaf node, and there is no merging done
        if return [1, 0] => the function was called on a leaf node and that leaf node merged with its right sibling
        if return [1, 1] => the function was called on a leaf node and that leaf node merged with its left sibling
        if return [2] => the function was called on a non-leaf node and there was no merging done
        if return [3, 0] => the function was called on a non-leaf node and that non-leaf node merged with its right sibling
        if return [3, 1] => the function was called on a non-leaf node and that non-leaf node merged with its left sibling
        """
        # BASE CASE
        if self.leaf:
            exists = False
            for i in range(len(self.keys)):
                if self.keys[i] == key:
                    exists = True
                    self.keys.pop(i)
                    self.pointers[i].deallocate()
                    self.pointers.pop(i)
                    break
            if not exists:
                return [4]
            # at this point, the pointer and key in leaf node is removed
            # check to see if min num keys for leaf is satisfied
            if len(self.keys) >= self.min_leaf_keys:
                self.flush_to_disk()
                return [0]
            # we know min leaf key is not satisfied but its ok for root
            if parent == None:
                self.flush_to_disk()
                return [0]
            # check if can take from left neighbour
            if left_neighbour and len(left_neighbour.keys) > self.min_leaf_keys:
                self.keys = [left_neighbour.keys.pop()] + self.keys
                self.pointers = [left_neighbour.pointers.pop(-2)] + self.pointers
                self.flush_to_disk()
                left_neighbour.flush_to_disk()
                return [0]
            # check if can take from right neighbour
            if right_neighbour and len(right_neighbour.keys) > self.min_leaf_keys:
                self.keys += [right_neighbour.keys.pop(0)]
                self.pointers = self.pointers[:-1] + [right_neighbour.pointers.pop(0)] + [self.pointers[-1]]
                self.flush_to_disk()
                right_neighbour.flush_to_disk()
                return [0]
            # if neither works, merge
            # try merge right
            if right_neighbour:
                assert len(self.keys) + len(right_neighbour.keys) <= self.max_keys
                assert type(self.pointers.pop()) == BPTreeNode
                self.keys += right_neighbour.keys
                self.pointers += right_neighbour.pointers
                self.flush_to_disk()
                return [1, 0]
            # try merge left
            elif left_neighbour:
                assert len(self.keys) + len(left_neighbour.keys) <= self.max_keys
                self.keys = left_neighbour.keys + self.keys
                self.pointers = left_neighbour.pointers[:-1] + self.pointers
                self.flush_to_disk()
                return [1, 1]
            else:
                raise Exception("Could neither borrow nor merge")
        
        # RECURSIVE
        else:
            pos = None
            for i in range(len(self.keys)):
                if key < self.keys[i]:
                    pos = i
                    break
            if pos == None:
                pos = len(self.keys)
            # pointers[pos] is the node we recursively call on
            # get the left and right sibling of the node we recursively call on so its easier to do merging/borrowing
            call_left_neighbour = BPTreeNode(block_id=self.pointers[pos-1]) if pos-1 >= 0 else None
            call_right_neighbour = BPTreeNode(block_id=self.pointers[pos+1]) if pos+1 < len(self.pointers) else None
            # get lower/upper bound which is needed for merging
            if 0 <= pos < len(self.keys):
                call_upper_bound = self.keys[pos]
            else:
                call_upper_bound = upper_bound
            if 0 <= pos - 1 < len(self.keys):
                call_lower_bound = self.keys[pos-1]
            else:
                call_lower_bound = lower_bound
            # res is the result from the recursive call
            res = BPTreeNode(block_id=self.pointers[pos]).delete(key,
                call_left_neighbour,
                call_right_neighbour,
                self,
                call_upper_bound,
                call_lower_bound)
            if res[0] == 4:
                # key not found
                return [4]
            if res[0] == 0:
                # res == 0 means that there was no merging from leaves
                # simply need to update the value of keys[pos-1] and keys[pos]
                if pos < len(self.keys):
                    self.keys[pos] = BPTreeNode(block_id=self.pointers[pos+1]).keys[0]
                if pos-1 >= 0:
                    self.keys[pos-1] = BPTreeNode(block_id=self.pointers[pos]).keys[0]
                self.flush_to_disk()
                return [2]
            elif res[0] == 2:
                for ppos in range(pos-1, pos+2):
                    if 1 <= ppos < len(self.pointers):
                        self.keys[ppos-1] = BPTreeNode(block_id=self.pointers[ppos]).find_min()
                self.flush_to_disk()
                return [2]
            else:
                if res[1] == 0:
                    # below merged right
                    # delete self.pointers[pos+1] and self.keys[pos]
                    self.pointers[pos+1].deallocate()
                    self.pointers.pop(pos+1)
                    self.keys.pop(pos)
                elif res[1] == 1:
                    # below merged left
                    # delete self.pointers[pos-1] and self.keys[pos-1]
                    self.pointers[pos-1].deallocate()
                    self.pointers.pop(pos-1)
                    self.keys.pop(pos-1)
                for ppos in range(pos-1, pos+2):
                    if 1 <= ppos < len(self.pointers):
                        self.keys[ppos-1] = BPTreeNode(block_id=self.pointers[ppos]).find_min()
                # check if non leaf min keys is satisfied
                if len(self.keys) >= self.min_non_leaf_keys or parent == None:
                    self.flush_to_disk()
                    return [2]
                # borrow from left
                if left_neighbour and len(left_neighbour.keys) > self.min_non_leaf_keys:
                    self.keys = [left_neighbour.keys.pop()] + self.keys
                    self.pointers = [left_neighbour.pointers.pop()] + self.pointers
                    self.keys[0] = BPTreeNode(block_id=self.pointers[1]).keys[0]
                    self.flush_to_disk()
                    left_neighbour.flush_to_disk()
                    return [2]
                # borrow from right
                if right_neighbour and len(right_neighbour.keys) > self.min_non_leaf_keys:
                    self.keys += [right_neighbour.keys.pop(0)]
                    self.pointers += [right_neighbour.pointers.pop(0)]
                    self.keys[-1] = BPTreeNode(block_id=self.pointers[-1]).keys[0]
                    self.flush_to_disk()
                    right_neighbour.flush_to_disk()
                    return [2]
                # if neither works, merge
                if right_neighbour:
                    assert upper_bound != float("inf")
                    x = len(self.keys)
                    self.keys += [upper_bound] + right_neighbour.keys
                    self.pointers += right_neighbour.pointers
                    self.keys[x] = BPTreeNode(block_id=self.pointers[x+1]).find_min()
                    self.flush_to_disk()
                    return [3, 0]
                elif left_neighbour:
                    assert lower_bound != float("-inf")
                    self.keys = left_neighbour.keys + [lower_bound] + self.keys
                    self.pointers = left_neighbour.pointers + self.pointers
                    self.keys[len(left_neighbour.keys)] = BPTreeNode(block_id=self.pointers[len(left_neighbour.keys)+1]).find_min()
                    self.flush_to_disk()
                    return [3, 1]
                else:
                    raise Exception("Could neither borrow nor merge")
    
    def validate(self, is_root=False):
        # assume that the tree has >1 node
        # check number of keys and pointers are valid
        if is_root:
            pass
        elif self.leaf:
            assert self.min_leaf_keys <= len(self.keys) <= self.max_keys
        else:
            assert self.min_non_leaf_keys <= len(self.keys) <= self.max_keys
        assert len(self.pointers) - len(self.keys) == 1
        # check that values in a node are sorted
        for i in range(len(self.keys) - 1):
            assert self.keys[i] <= self.keys[i+1]
        if self.leaf:
            # already reach leaf nodes
            return self.keys[0]
        # check that values in a level are sorted
        for i in range(len(self.pointers)-1):
            assert BPTreeNode(block_id=self.pointers[i]).keys[0] <= BPTreeNode(block_id=self.pointers[i+1]).keys[0]
        # recursively check
        for i in range(len(self.pointers)):
            if self.pointers[i] == None:
                continue
            min_val = BPTreeNode(block_id=self.pointers[i]).validate()
            if i > 0:
                assert self.keys[i-1] == min_val
        return BPTreeNode(block_id=self.pointers[0]).validate()
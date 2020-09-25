from utils import *
from structures import *

class Node:
    def __init__(self, max_keys=3): # max_keys = (len(block) - 25) // 22
        self.block_id = Disk.get_next_free()
        self.parent = None
        self.leaf = True

        self.keys = []
        self.pointers = [None] # len(pointers) is always len(keys) + 1

        self.max_keys = max_keys
        self.min_leaf_keys = (self.max_keys + 1) // 2
        self.min_non_leaf_keys = self.max_keys // 2
        
    def get_right_sibling(self):
        if self.parent == None:
            return None
        for i in range(len(self.parent.pointers)-1):
            if self.parent.pointers[i] is self:
                return self.parent.pointers[i+1]
        return None

    def get_left_sibling(self):
        if self.parent == None:
            return None
        for i in range(1, len(self.parent.pointers)):
            if self.parent.pointers[i] is self:
                return self.parent.pointers[i-1]
        return None

    def flush_to_disk(self):
        if self.block_id == 0:
            raise Exception("Block id of 0 is forbidden")
        block = Disk.read_block(self.block_id)
        parent_block_id = self.parent.block_id if self.parent else 0
        if self.leaf:
            set_index_block_header(block, "leaf", self.block_id, parent_block_id)
        else:
            set_index_block_header(block, "non-leaf", self.block_id, parent_block_id)
        pointers = []
        for p in self.pointers:
            if p == None:
                pointers.append(None) # logic in utils will change to 0, 0
            elif type(p) == Node:
                pointers.append((p.block_id, 0)) # pointers to index dont need offset, let it be 0
            else:
                pointers.append(p)
        set_ptrs_keys_bytes(block, serialize_ptrs_keys(pointers, self.keys))
        Disk.write_block(self.block_id, block)
        if not self.leaf:
            for i in range(len(self.pointers)):
                if self.pointers[i]:
                    self.pointers[i].flush_to_disk()
    
    def deallocate(self):
        Disk.deallocate(self.block_id)
    
    def remove_from_parent_next_pointer_and_key(self):
        for i in range(len(self.parent.pointers)-1):
            if self.parent.pointers[i] is self:
                self.parent.pointers.pop(i+1) # alr deallocated in leaf merge
                return self.parent.keys.pop(i)
            
    def remove_from_parent_prev_pointer_and_key(self):
        for i in range(1, len(self.parent.pointers)):
            if self.parent.pointers[i] is self:
                self.parent.pointers.pop(i-1)
                return self.parent.keys.pop(i-1)

    def replace_key(self, old, new):
        if new == None:
            return
        for i in range(len(self.keys)):
            if self.keys[i] == old:
                self.keys[i] = new

    def distribute(self, right):
        # print("distribute")   
        pivot_pos = None
        for i in range(len(self.parent.pointers)):
            if self.parent.pointers[i] is self:
                pivot_pos = i
        if pivot_pos == None or pivot_pos == len(self.parent.pointers) - 1:
            print("pivot_pos:", pivot_pos)
            raise Exception("If left has a right sibling, it must have a pivot_pos < len(parent.pointers) - 1")
        assert self.parent.pointers[pivot_pos+1] is right
        
        num_left = (len(right.keys) + len(self.keys) + 1) // 2
        num_right = (len(right.keys) + len(self.keys)) - num_left
        
        if num_left > len(self.keys):
            for _ in range(num_left - len(self.keys)):
                self.keys.append(self.parent.keys[pivot_pos])
                self.parent.keys[pivot_pos] = right.keys.pop(0)
                self.pointers.append(right.pointers.pop(0))
                self.pointers[-1].parent = self

        else:
            for _ in range(num_right - len(right.keys)):
                right.keys.insert(0, self.parent.keys[pivot_pos])
                self.parent.keys[pivot_pos] = self.keys.pop()
                right.pointers.insert(0, self.pointers.pop())
                right.pointers[0].parent = right

    def merge_with_right(self, right):
        self.keys.append(self.remove_from_parent_next_pointer_and_key())
        for i in range(len(right.pointers)):
            self.pointers.append(right.pointers[i])
            self.pointers[-1].parent = self
            if i == len(right.keys): # consider the fact that there is 1 more pointer than key
                break
            self.keys.append(right.keys[i])

    def merge_with_left(self, left):
        self.keys.insert(0, self.remove_from_parent_prev_pointer_and_key())
        while left.pointers:
            self.pointers.insert(0, left.pointers.pop())
            self.pointers[0].parent = self
            if left.keys: # consider the fact that there is 1 more pointer than key
                self.keys.insert(0, left.keys.pop())
    
    def leaf_distribute(self, right):
        # print("leaf_distribute")
        all_keys = self.keys + right.keys
        all_pointers = self.pointers[:-1] + right.pointers[:-1]
        num_left = (len(all_keys) + 1) // 2

        self.keys = all_keys[:num_left]
        self.pointers = all_pointers[:num_left] + [self.pointers[-1]]

        right.keys = all_keys[num_left:]
        right.pointers = all_pointers[num_left:] + [right.pointers[-1]]
        
        self.update_parent_lb()
        right.update_parent_lb()

    def leaf_merge(self, right): # seems like its symmetric - need further test
        # print("leaf_merge")
        self.keys.extend(right.keys)
        self.pointers.pop()
        self.pointers.extend(right.pointers)
        self.remove_from_parent_next_pointer_and_key()
        self.update_parent_lb() # dunno if needed

    def update_parent_lb(self):
        for i in range(1, len(self.parent.pointers)):
            if self.parent.pointers[i] is self:
                self.parent.keys[i-1] = self.keys[0]

    def delete(self, key):
        if self.leaf:
            for i in range(len(self.keys)):
                if self.keys[i] == key:
                    self.keys.pop(i)
                    data_block_id, offset = self.pointers.pop(i)
                    data_block = Disk.read_block(data_block_id)
                    assert get_block_type(data_block) == "data"
                    delete_record_bytes(data_block, offset)
                    Disk.write_block(data_block_id, data_block)
                    # TODO: add to queue
                    next_largest = self.keys[i] if i < len(self.keys) else None
                    break
            if next_largest == None:
                next_largest = self.pointers[-1].keys[0] if self.pointers[-1] else None

            if self.parent == None:
                return False, next_largest
            
            if len(self.keys) >= self.min_leaf_keys:
                return False, next_largest
            
            # print("Leaf underflow")
            # check if can borrow from left sibling
            left_sibling = self.get_left_sibling()
            if left_sibling and len(left_sibling.keys) > self.min_leaf_keys:
                # print("Leaf borrow from left")
                left_sibling.leaf_distribute(self)
                return False, next_largest
            
            # check if can borrow from right sibling
            right_sibling = self.get_right_sibling()
            if right_sibling and len(right_sibling.keys) > self.min_leaf_keys:
                # print("Leaf borrow from right")
                self.leaf_distribute(right_sibling)
                return False, next_largest
            
            # check if can merge with left sibling
            if left_sibling:
                # print("Leaf merge with left")
                left_sibling.leaf_merge(self)
                return True, next_largest
            
            # check if can merge with right sibling
            if right_sibling:
                # print("Leaf merge with right")
                self.leaf_merge(right_sibling)
                return True, next_largest
            
            raise Exception("Leaf deletion underflow could never borrow nor merge")
                
        elif not self.leaf:
            deleted = False
            for i in range(len(self.keys)):
                if key < self.keys[i]:
                    pos = i
                    res = self.pointers[i].delete(key)
                    deleted = True
                    break
            
            if not deleted:
                pos = len(self.pointers) - 1
                res = self.pointers[-1].delete(key)
            
            if res[0] == False or len(self.keys) >= self.min_non_leaf_keys or self.parent == None:
                self.replace_key(key, res[1])
                return False, res[1]
                
            # print("Non leaf underflow")
            # check if can borrow from left sibling
            left_sibling = self.get_left_sibling()
            if left_sibling and len(left_sibling.keys) > self.min_non_leaf_keys:
                # print("Non leaf borrow from left")
                left_sibling.distribute(self)
                self.replace_key(key, res[1])
                return False, res[1]
            
            right_sibling = self.get_right_sibling()
            if right_sibling and len(right_sibling.keys) > self.min_non_leaf_keys:
                # print("Non leaf borrow from right")
                self.distribute(right_sibling)
                self.replace_key(key, res[1])
                return False, res[1]
            
            # check if can merge with left sibling
            if left_sibling:
                # print("Non leaf merge with left")
                self.merge_with_left(left_sibling)
                self.replace_key(key, res[1])
                return True, res[1]
            
            # check if can merge with right sibling
            if right_sibling:
                # print("Non leaf merge with right")
                self.merge_with_right(right_sibling)
                self.replace_key(key, res[1])
                return True, res[1]
            
            raise Exception("Non leaf deletion underflow could never borrow nor merge")

    def insert(self, key, value):
        if self.leaf:
            inserted = False
            for i in range(len(self.keys)):
                if key < self.keys[i]:
                    self.keys.insert(i, key)
                    self.pointers.insert(i, value)
                    inserted = True
                    break
            if not inserted:
                self.pointers.insert(len(self.keys), value)
                self.keys.insert(len(self.keys), key)
            if len(self.keys) > self.max_keys:
                num_left = (len(self.keys) + 1) // 2
                
                right_node = Node()
                right_node.keys = self.keys[num_left:]
                right_node.pointers = self.pointers[num_left:]
                
                self.keys = self.keys[:num_left]
                self.pointers = self.pointers[:num_left]
                self.pointers.append(right_node)
                
                to_insert = Node()
                to_insert.leaf = False
                to_insert.keys = [right_node.keys[0]]
                to_insert.pointers = [self, right_node]
                
                self.parent = to_insert
                right_node.parent = to_insert
                
                return to_insert
            
            return None
            
        elif not self.leaf:
            inserted = False
            for i in range(len(self.keys)):
                if key < self.keys[i]:
                    pos = i
                    res = self.pointers[i].insert(key, value)
                    inserted = True
                    break
            if not inserted:
                pos = len(self.pointers) - 1
                res = self.pointers[-1].insert(key, value)

            if res == None:
                return None
            
            self.keys.insert(pos, res.keys[0])
            self.pointers[pos] = res.pointers[0]
            self.pointers[pos].parent = self
            
            self.pointers.insert(pos+1, res.pointers[1])
            self.pointers[pos+1].parent = self
            
            if len(self.keys) > self.max_keys:
                num_left = len(self.keys) // 2
                
                right_node = Node()
                right_node.leaf = False
                right_node.keys = self.keys[num_left+1:]
                right_node.pointers = self.pointers[num_left+1:]
                for pointer in right_node.pointers:
                    pointer.parent = right_node
                
                to_insert = Node()
                to_insert.leaf = False
                to_insert.keys = [self.keys[num_left]]
                to_insert.pointers = [self, right_node]
                
                self.keys = self.keys[:num_left]
                self.pointers = self.pointers[:num_left+1]
                
                self.parent = to_insert
                right_node.parent = to_insert
                
                return to_insert

            return None

    def validate(self):
        # asserts that all nodes have neither overflow nor underflow
        # asserts that for every non-leaf node, its children points to itself as a parent
        # asserts that all keys in a node are sorted
        # asserts that all keys in a level are sorted
        # asserts that root.keys[i] == min val in the subtree pointed by root.pointers[i+1]
        if self.parent != None:
            if self.leaf:
                assert self.min_leaf_keys <= len(self.keys) <= self.max_keys
            else:
                assert self.min_non_leaf_keys <= len(self.keys) <= self.max_keys
        if not self.leaf:
            for p in self.pointers:
                assert p.parent is self
        for i in range(len(self.keys)-1):
            assert self.keys[i] < self.keys[i+1]
        if self.leaf:
            return self.keys[0]
        for i in range(len(self.pointers)-1):
            assert self.pointers[i].keys[0] < self.pointers[i+1].keys[0]
        for i in range(len(self.pointers)):
            if i > 0:
                assert self.keys[i-1] == self.pointers[i].validate()
            else:
                self.pointers[i].validate()
        return self.pointers[0].validate()

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
            return self.pointers[-1], 0
        else:
            # find the subtree to recursively call on
            for i in range(len(self.keys)):
                if key < self.keys[i]:
                    return self.pointers[i].search_first_gte(key)
            return self.pointers[-1].search_first_gte(key)

    def get_num_nodes(self):
        if self.leaf:
            return 1
        return 1 + sum(self.pointers[i].get_num_nodes() for i in range(len(self.pointers)))

    def get_height(self):
        res = 0
        cur = self
        while cur and type(cur) == Node:
            res += 1
            cur = cur.pointers[0]
        return res

    def search_range(self, lower, upper, return_key=False):
        """
        Returns a list of all values whose keys are in the range [lower, upper] inclusive
        If lower is None, it is treated as no lower bound
        If upper is None, it is trated as no upper bound
        If both are None, return all values
        """
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
                if return_key:
                    res.append(node.keys[i])
                else:
                    res.append(node.pointers[i])
            # move to the immediate right neighbour
            if node.pointers[-1] == None:
                return res
            node = node.pointers[-1]
            pos = 0
        # this return is needed if the res includes the rightmost leaf node
        return res

class Tree:
    def __init__(self):
        self.root = Node()
    
    def _delete(self, key):
        self.root.delete(key)
        if self.root.pointers[0] == None:
            print("tree is empty")
        if len(self.root.keys) == 0:
            print("root is empty, shrinking tree level")
            self.root = self.root.pointers[0]
            if self.root:
                self.root.parent = None
            else:
                # design choice to populate null tree with 1 empty node
                self.root = Node()

    def insert(self, augmented_key, value):
        # CLIENT API
        res = self.root.insert(augmented_key, value)
        if res != None:
            self.root = res

    def search(self, key, return_key=False):
        # CLIENT API
        return self.root.search_range((key, ""), (key, chr(255)), return_key)

    def search_range(self, lower, upper):
        # CLIENT API
        if lower == None:
            lower = float("-inf")
        if upper == None:
            upper = float("inf")
        return self.root.search_range((lower, ""), (upper, chr(255)))

    def delete(self, key):
        # CLIENT API
        to_delete = self.search(key, True)
        for k in to_delete:
            self._delete(k)

    def show(self):
        # CLIENT API
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
                    if node.leaf:
                        break
                    nxt.append(pointer)
            print(" ".join(str(x) for x in to_print))
            print()
            cur = nxt
    
    def validate(self):
        # CLIENT API
        self.root.validate()

    def get_num_nodes(self):
        # CLIENT API
        return self.root.get_num_nodes()

    def get_height(self):
        # CLIENT API
        return self.root.get_height()

    def save(self):
        # CLIENT API
        self.root.flush_to_disk()

from utils import *
from structures import *

class Node:
    def __init__(self, block_id=None):
        # if block_id is given, means block has been allocated before, just grab from disk
        # if not allocate a block
        # then populate the fields
        if block_id == None:
            # print("Allocating index block")
            block_id = Disk.get_next_free()
            block = Disk.read_block(block_id)
            set_index_block_header(block, "leaf", block_id, None)
        else:
            block = Disk.read_block(block_id)
        index_type, block_id, parent_block_id, num_keys, key_size = get_index_block_header(block)
        
        if get_block_type(block) == "data":
            raise Exception("Cannot instantiate node with data block")

        self.block = block
        self.block_id = block_id
        self.parent = None if parent_block_id == 0 else parent_block_id # reference to parent node
        self.leaf = index_type == 3 # whether node is leaf node

        pointers, keys = deserialize_index_block(block)
        self.keys = keys
        self.pointers = pointers # len(pointers) is always len(keys) + 1
        if self.pointers[-1] == (0, 0):
            self.pointers[-1] = None

        self.max_keys = (len(block) - 25) // 22
        self.min_leaf_keys = (self.max_keys + 1) // 2
        self.min_non_leaf_keys = self.max_keys // 2
        
        self.flush_to_disk()

    def get_right_sibling(self):
        if self.parent == None:
            return None
        parent = Node(block_id=self.parent)
        for i in range(len(parent.pointers)-1):
            if parent.pointers[i][0] == self.block_id:
                return Node(block_id=parent.pointers[i+1][0])
        return None

    def get_left_sibling(self):
        if self.parent == None:
            return None
        parent = Node(block_id=self.parent)
        for i in range(1, len(parent.pointers)):
            if parent.pointers[i][0] == self.block_id:
                return Node(block_id=parent.pointers[i-1][0])
        return None

    def flush_to_disk(self):
        if self.block_id == 0:
            raise Exception("Block id of 0 is forbidden")
        if self.leaf:
            set_index_block_header(self.block, "leaf", self.block_id, self.parent)
        else:
            set_index_block_header(self.block, "non-leaf", self.block_id, self.parent)
        set_ptrs_keys_bytes(self.block, serialize_ptrs_keys(self.pointers, self.keys))
        Disk.write_block(self.block_id, self.block)
    
    def deallocate(self):
        Disk.deallocate(self.block_id)

def delete_helper(node, key):
    if node.leaf:
        for i in range(len(node.keys)):
            if node.keys[i] == key:
                node.keys.pop(i)
                data_block_id, offset = node.pointers.pop(i)
                data_block = Disk.read_block(data_block_id)
                assert get_block_type(data_block) == "data"
                delete_record_bytes(data_block, offset)
                Disk.write_block(data_block_id, data_block)
                # TODO: add to queue
                next_largest = node.keys[i] if i < len(node.keys) else None
                break
        if next_largest == None:
            next_largest = Node(block_id=node.pointers[-1][0]).keys[0] if node.pointers[-1] else None
        
        node.flush_to_disk()

        if node.parent == None:
            return False, next_largest
        
        if len(node.keys) >= node.min_leaf_keys:
            return False, next_largest
        
#         print("Leaf underflow")
        # check if can borrow from left sibling
        left_sibling = node.get_left_sibling()
        if left_sibling and len(left_sibling.keys) > node.min_leaf_keys:
            # print("Leaf borrow from left")
            leaf_distribute(left_sibling, node)
            return False, next_largest
        
        # check if can borrow from right sibling
        right_sibling = node.get_right_sibling()
        if right_sibling and len(right_sibling.keys) > node.min_leaf_keys:
            # print("Leaf borrow from right")
            leaf_distribute(node, right_sibling)
            return False, next_largest
        
        # check if can merge with left sibling
        if left_sibling:
            # print("Leaf merge with left")
            leaf_merge(left_sibling, node)
            return True, next_largest
        
        # check if can merge with right sibling
        if right_sibling:
            # print("Leaf merge with right")
            leaf_merge(node, right_sibling)
            return True, next_largest
        
        raise Exception("Leaf deletion underflow could never borrow nor merge")
            
    elif not node.leaf:
        deleted = False
        for i in range(len(node.keys)):
            if key < node.keys[i]:
                pos = i
                res = delete_helper(Node(block_id=node.pointers[i][0]), key)
                deleted = True
                break
        
        if not deleted:
            pos = len(node.pointers) - 1
            res = delete_helper(Node(block_id=node.pointers[-1][0]), key)
        
        node = Node(node.block_id) # refresh
        if res[0] == False or len(node.keys) >= node.min_non_leaf_keys or node.parent == None:
            replace_key(node, key, res[1])
            return False, res[1]
            
#         print("Non leaf underflow")
        # check if can borrow from left sibling
        left_sibling = node.get_left_sibling()
        if left_sibling and len(left_sibling.keys) > node.min_non_leaf_keys:
            # print("Non leaf borrow from left")
            distribute(left_sibling, node)
            replace_key(node, key, res[1])
            return False, res[1]
        
        right_sibling = node.get_right_sibling()
        if right_sibling and len(right_sibling.keys) > node.min_non_leaf_keys:
            # print("Non leaf borrow from right")
            distribute(node, right_sibling)
            replace_key(node, key, res[1])
            return False, res[1]
        
        # check if can merge with left sibling
        if left_sibling:
            # print("Non leaf merge with left")
            merge_with_left(left_sibling, node)
            replace_key(node, key, res[1])
            return True, res[1]
        
        # check if can merge with right sibling
        if right_sibling:
            # print("Non leaf merge with right")
            merge_with_right(node, right_sibling)
            replace_key(node, key, res[1])
            return True, res[1]
        
        raise Exception("Non leaf deletion underflow could never borrow nor merge")

def replace_key(node, old, new):
    if new == None:
        return
    for i in range(len(node.keys)):
        if node.keys[i] == old:
            node.keys[i] = new
    node.flush_to_disk()

def merge_with_right(node, right):
    x = remove_from_parent_next_pointer_and_key(node)
    node.keys.append(x)
    for i in range(len(right.pointers)):
        node.pointers.append(right.pointers[i])
        node_appended = Node(block_id=node.pointers[-1][0])
        node_appended.parent = node.block_id
        node_appended.flush_to_disk()

        if i == len(right.keys):
            break
        node.keys.append(right.keys[i])
    node.flush_to_disk()

def merge_with_left(left, node):
    x = remove_from_parent_prev_pointer_and_key(node)
    node.keys.insert(0, x)
    while left.pointers:
        node.pointers.insert(0, left.pointers.pop())
        node_appended = Node(block_id=node.pointers[0][0])
        node_appended.parent = node.block_id
        node_appended.flush_to_disk()

        if left.keys:
            node.keys.insert(0, left.keys.pop())
    node.flush_to_disk()

def insert_helper(node, key, value):
    if node.leaf:
        inserted = False
        for i in range(len(node.keys)):
            if key < node.keys[i]:
                node.keys.insert(i, key)
                node.pointers.insert(i, value)
                inserted = True
                break
        if not inserted:
            node.pointers.insert(len(node.keys), value)
            node.keys.insert(len(node.keys), key)
        if len(node.keys) > node.max_keys:
            num_left = (len(node.keys) + 1) // 2
            
            right_node = Node()
            right_node.keys = node.keys[num_left:]
            right_node.pointers = node.pointers[num_left:]
            
            node.keys = node.keys[:num_left]
            node.pointers = node.pointers[:num_left] + [(right_node.block_id, 0)]
            
            to_insert = Node()
            to_insert.leaf = False
            to_insert.keys = [right_node.keys[0]]
            to_insert.pointers = [(node.block_id, 0), (right_node.block_id, 0)]
            
            node.parent = to_insert.block_id
            right_node.parent = to_insert.block_id
            
            right_node.flush_to_disk()
            to_insert.flush_to_disk()
            node.flush_to_disk()
            return to_insert
        
        node.flush_to_disk()
        return None
        
    elif not node.leaf:
        inserted = False
        for i in range(len(node.keys)):
            if key < node.keys[i]:
                pos = i
                res = insert_helper(Node(node.pointers[i][0]), key, value)
                inserted = True
                break
        if not inserted:
            pos = len(node.pointers) - 1
            res = insert_helper(Node(node.pointers[-1][0]), key, value)

        if res == None:
            return None
        
        node.keys.insert(pos, res.keys[0])
        node.pointers[pos] = res.pointers[0]
        node_at_pos = Node(block_id=node.pointers[pos][0])
        node_at_pos.parent = node.block_id
        node_at_pos.flush_to_disk()
        
        node.pointers.insert(pos+1, res.pointers[1])
        node_at_pos_plus_1 = Node(block_id=node.pointers[pos+1][0])
        node_at_pos_plus_1.parent = node.block_id
        node_at_pos_plus_1.flush_to_disk()
        
        # res.deallocate()
        
        if len(node.keys) > node.max_keys:
            num_left = len(node.keys) // 2
            
            right_node = Node()
            right_node.leaf = False
            right_node.keys = node.keys[num_left+1:]
            right_node.pointers = node.pointers[num_left+1:]
            for pointer in right_node.pointers:
                node_moved = Node(block_id=pointer[0])
                node_moved.parent = right_node.block_id
                node_moved.flush_to_disk()
            
            to_insert = Node()
            to_insert.leaf = False
            to_insert.keys = [node.keys[num_left]]
            to_insert.pointers = [(node.block_id, 0), (right_node.block_id, 0)]
            
            node.keys = node.keys[:num_left]
            node.pointers = node.pointers[:num_left+1]
            
            node.parent = to_insert.block_id
            right_node.parent = to_insert.block_id
            
            right_node.flush_to_disk()
            to_insert.flush_to_disk()
            node.flush_to_disk()
            return to_insert

        node.flush_to_disk()
        return None



def leaf_distribute(left, right):
#     print("leaf_distribute")
    all_keys = left.keys + right.keys
    all_pointers = left.pointers[:-1] + right.pointers[:-1]
    num_left = (len(all_keys) + 1) // 2

    left.keys = all_keys[:num_left]
    left.pointers = all_pointers[:num_left] + [left.pointers[-1]]

    right.keys = all_keys[num_left:]
    right.pointers = all_pointers[num_left:] + [right.pointers[-1]]
    
    left.flush_to_disk()
    right.flush_to_disk()
    
    update_parent_lb(left)
    update_parent_lb(right)
    

def update_parent_lb(node):
    parent = Node(block_id=node.parent)
    for i in range(1, len(parent.pointers)):
        if parent.pointers[i][0] == node.block_id:
            parent.keys[i-1] = node.keys[0]
    parent.flush_to_disk()

def leaf_merge(left, right):
#     print("leaf_merge")
    left.keys.extend(right.keys)
    left.pointers.pop()
    left.pointers.extend(right.pointers)
    left.flush_to_disk()
    # right.deallocate()
    remove_from_parent_next_pointer_and_key(left)
    update_parent_lb(left)

def remove_from_parent_next_pointer_and_key(node):
    parent = Node(block_id=node.parent)
    for i in range(len(parent.pointers)-1):
        if parent.pointers[i][0] == node.block_id:
            parent.pointers.pop(i+1) # alr deallocated in leaf merge
            k = parent.keys.pop(i)
            parent.flush_to_disk()
            return k
        
def remove_from_parent_prev_pointer_and_key(node):
    parent = Node(block_id=node.parent)
    for i in range(1, len(parent.pointers)):
        if parent.pointers[i][0] == node.block_id:
            parent.pointers.pop(i-1)
            k = parent.keys.pop(i-1)
            parent.flush_to_disk()
            return k

def distribute(left, right):
#     print("distribute")
    parent = Node(block_id=left.parent)
    pivot_pos = None
    for i in range(len(parent.pointers)):
        if parent.pointers[i][0] == left.block_id:
            pivot_pos = i
    if pivot_pos == None or pivot_pos == len(parent.pointers) - 1:
        print("pivot_pos:", pivot_pos)
        raise Exception("If left has a right sibling, it must have a pivot_pos < len(parent.pointers) - 1")
    assert parent.pointers[pivot_pos+1][0] == right.block_id
    
    num_left = (len(right.keys) + len(left.keys) + 1) // 2
    num_right = (len(right.keys) + len(left.keys)) - num_left
    
    if num_left > len(left.keys):
        for _ in range(num_left - len(left.keys)):
            left.keys.append(parent.keys[pivot_pos])
            parent.keys[pivot_pos] = right.keys.pop(0)
            left.pointers.append(right.pointers.pop(0))
            appended_node = Node(block_id=left.pointers[-1][0])
            appended_node.parent = left.block_id
            appended_node.flush_to_disk()
        left.flush_to_disk()
        right.flush_to_disk()
        parent.flush_to_disk()

    else:
        for _ in range(num_right - len(right.keys)):
            right.keys.insert(0, parent.keys[pivot_pos])
            parent.keys[pivot_pos] = left.keys.pop()
            right.pointers.insert(0, left.pointers.pop())
            appended_node = Node(block_id=right.pointers[0][0])
            appended_node.parent = right.block_id
            appended_node.flush_to_disk()
        left.flush_to_disk()
        right.flush_to_disk()
        parent.flush_to_disk()

def validate_parent_helper(root):
    # asserts that for every non-leaf node, its children points to itself as a parent
    if root.leaf:
        return
    for p in root.pointers:
        child = Node(block_id=p[0])
        assert child.parent == root.block_id
        validate_parent_helper(child)
        
def validate_helper(root):
    # asserts that all nodes have neither overflow nor underflow
    # asserts that all keys in a node are sorted
    # asserts that all keys in a level are sorted
    # asserts that root.keys[i] == min val in the subtree pointed by root.pointers[i+1]
    if root.parent != None:
        if root.leaf:
            assert root.min_leaf_keys <= len(root.keys) <= root.max_keys
        else:
            assert root.min_non_leaf_keys <= len(root.keys) <= root.max_keys
    for i in range(len(root.keys)-1):
        assert root.keys[i] < root.keys[i+1]
    if root.leaf:
        return root.keys[0]
    for i in range(len(root.pointers)-1):
        assert Node(block_id=root.pointers[i][0]).keys[0] < Node(block_id=root.pointers[i+1][0]).keys[0]
    for i in range(len(root.pointers)):
        if i > 0:
            assert root.keys[i-1] == validate_helper(Node(block_id=root.pointers[i][0]))
        else:
            validate_helper(Node(block_id=root.pointers[i][0]))
    return validate_helper(Node(block_id=root.pointers[0][0]))

def search_first_gte(root, key):
    """
    A utility function used by search_range to return the first leaf node >= key
    If found, return the leaf node containing the key and the index of the key in the node
    If not found, i.e. key is smaller than all keys, return None
    """
    if root.leaf:
        for i in range(len(root.keys)):
            if root.keys[i] >= key:
                return root, i
        if root.pointers[-1] == None:
            # this is true if self is the rightmost leaf node
            return None
        # if leaf node is not rightmost, we know the first key of the immediate right neightbour will satisfy condition
        # because self.pointers[-1].keys[0] >= some LB > key
        return Node(block_id=root.pointers[-1][0]), 0
    else:
        # find the subtree to recursively call on
        for i in range(len(root.keys)):
            if key < root.keys[i]:
                return search_first_gte(Node(block_id=root.pointers[i][0]), key)
        return search_first_gte(Node(root.pointers[-1][0]), key)

def get_num_nodes_helper(root):
    if root.leaf:
        return 1
    return 1 + sum(get_num_nodes_helper(Node(block_id=root.pointers[i][0])) for i in range(len(root.pointers)))

class Tree:
    def __init__(self):
        self.root = Node()
    
    def _search_range(self, lower, upper, return_key=False):
        """
        Returns a list of all values whose keys are in the range [lower, upper] inclusive
        If lower is None, it is treated as no lower bound
        If upper is None, it is trated as no upper bound
        If both are None, return all values
        """
        if lower > upper:
            return []
        
        first_gte = search_first_gte(self.root, lower)
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
            node = Node(node.pointers[-1][0])
            pos = 0
        # this return is needed if the res includes the rightmost leaf node
        return res

    def _delete(self, key):
        delete_helper(self.root, key)
        if self.root.pointers[0] == None:
            print("tree is empty")
        if len(self.root.keys) == 0:
            print("root is empty, shrinking tree level")
            new_root = Node(self.root.pointers[0][0])
            new_root.parent = None
            new_root.flush_to_disk()
            self.root = new_root

    def insert(self, augmented_key, value):
        # augmented_key is str(average_rating) + tconst
        res = insert_helper(self.root, augmented_key, value)
        if res != None:
            self.root = res

    def search(self, key, return_key=False):
        return self._search_range((key, ""), (key, chr(255)), return_key)

    def search_range(self, lower, upper):
        if lower == None:
            lower = float("-inf")
        if upper == None:
            upper = float("inf")
        return self._search_range((lower, ""), (upper, chr(255)))

    def delete(self, key):
        to_delete = self.search(key, True)
        # print("to_del:", to_delete)
        # assert len(to_delete) == len(set(to_delete))
        for k in to_delete:
            self._delete(k)

    def show(self):
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
                    nxt.append(Node(pointer[0]))
            print(" ".join(str(x) for x in to_print))
            print()
            cur = nxt
    
    def validate(self):
        validate_parent_helper(self.root)
        validate_helper(self.root)

    def get_num_nodes(self):
        return get_num_nodes_helper(self.root)

    def get_height(self):
        res = 1
        cur = self.root
        while not cur.leaf:
            res += 1
            cur = Node(cur.pointers[0][0])
        return res

# if __name__ == "__main__":
#     tree = Tree()
#     records = parse_data()
#     for record in records:
#         tree.insert(record[1], record)
#     print(tree.get_num_nodes())
#     print(tree.get_height())
#     tree.validate()
    
#     # experiment 3
#     exp3 = tree.search(8.0)
#     actual_exp3 = []
#     for record in records:
#         if record[1] == 8.0:
#             actual_exp3.append(record)
#     assert sorted(exp3) == sorted(actual_exp3)
#     tree.validate()

#     # experiment 4
#     exp4 = [record[0] for record in tree.search_range(7.0, 9.0)]
#     actual_exp4 = [record[0] for record in records if 7.0 <= record[1] <= 9.0]
#     assert sorted(exp4) == sorted(actual_exp4)
#     tree.validate()

#     # experiment 5
#     tree.delete(7.0)
#     records_remaining = tree.search_range(None, None)
#     actual_records_remaining = [record for record in records if record[1] != 7.0]
#     assert sorted(records_remaining) == sorted(actual_records_remaining)
#     tree.validate()
from utils import *
from structures import *

class Node:
    def __init__(self, block_id=None):
        # if block_id is given, means block has been allocated before, just grab from disk
        # if not allocate a block
        # then populate the fields
        if block_id == None:
            block_id = Disk.get_next_free()
            block = Disk.read_block(block_id)
            set_data_block_header(block, block_id)
        else:
            block = Disk.read_block(block_id)
        
        self.parent = None # reference to parent node
        self.leaf = True # whether node is leaf node
        self.keys = []
        self.pointers = [None] # len(pointers) is always len(keys) + 1
        self.max_keys = MAX_KEYS
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
    
    def deallocate(self):
        Disk.deallocate(self.block_id)

def delete_helper(node, key):
    if node.leaf:
        for i in range(len(node.keys)):
            if node.keys[i] == key:
                node.keys.pop(i)
                node.pointers.pop(i)
                next_largest = node.keys[i] if i < len(node.keys) else None
                break
        
        if next_largest == None:
            next_largest = node.pointers[-1].keys[0] if node.pointers[-1] else None
        
        if node.parent == None:
            return False, next_largest
        
        if len(node.keys) >= node.min_leaf_keys:
            return False, next_largest
        
#         print("Leaf underflow")
        # check if can borrow from left sibling
        left_sibling = node.get_left_sibling()
        if left_sibling and len(left_sibling.keys) > node.min_leaf_keys:
#             print("Leaf borrow from left")
            leaf_distribute(left_sibling, node)
            return False, next_largest
        
        # check if can borrow from right sibling
        right_sibling = node.get_right_sibling()
        if right_sibling and len(right_sibling.keys) > node.min_leaf_keys:
#             print("Leaf borrow from right")
            leaf_distribute(node, right_sibling)
            return False, next_largest
        
        # check if can merge with left sibling
        if left_sibling:
#             print("Leaf merge with left")
            leaf_merge(left_sibling, node)
            return True, next_largest
        
        # check if can merge with right sibling
        if right_sibling:
#             print("Leaf merge with right")
            leaf_merge(node, right_sibling)
            return True, next_largest
        
        raise Exception("Leaf deletion underflow could never borrow nor merge")
            
    elif not node.leaf:
        deleted = False
        for i in range(len(node.keys)):
            if key < node.keys[i]:
                pos = i
                res = delete_helper(node.pointers[i], key)
                deleted = True
                break
        
        if not deleted:
            pos = len(node.pointers) - 1
            res = delete_helper(node.pointers[-1], key)
        
        if res[0] == False or len(node.keys) >= node.min_non_leaf_keys or node.parent == None:
            replace_key(node, key, res[1])
            return False, res[1]
            
#         print("Non leaf underflow")
        # check if can borrow from left sibling
        left_sibling = node.get_left_sibling()
        if left_sibling and len(left_sibling.keys) > node.min_non_leaf_keys:
#             print("Non leaf borrow from left")
            distribute(left_sibling, node)
            replace_key(node, key, res[1])
            return False, res[1]
        
        right_sibling = node.get_right_sibling()
        if right_sibling and len(right_sibling.keys) > node.min_non_leaf_keys:
#             print("Non leaf borrow from right")
            distribute(node, right_sibling)
            replace_key(node, key, res[1])
            return False, res[1]
        
        # check if can merge with left sibling
        if left_sibling:
#             print("Non leaf merge with left")
            merge_with_left(left_sibling, node)
            replace_key(node, key, res[1])
            return True, res[1]
        
        # check if can merge with right sibling
        if right_sibling:
#             print("Non leaf merge with right")
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

def merge_with_right(node, right):
    x = remove_from_parent_next_pointer_and_key(node)
    node.keys.append(x)
    for i in range(len(right.pointers)):
        node.pointers.append(right.pointers[i])
        node.pointers[-1].parent = node
#         node.keys[-1] = node.pointers[-1].keys[0]
        if i == len(right.keys):
            break
        node.keys.append(right.keys[i])

def merge_with_left(left, node):
    x = remove_from_parent_prev_pointer_and_key(node)
    node.keys.insert(0, x)
    # right.keys[0] = right.pointers[0].keys[0]
    while left.pointers:
        node.pointers.insert(0, left.pointers.pop())
        node.pointers[0].parent = node
        if left.keys:
            node.keys.insert(0, left.keys.pop())

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
            node.pointers = node.pointers[:num_left] + [right_node]
            
            to_insert = Node()
            to_insert.leaf = False
            to_insert.keys = [right_node.keys[0]]
            to_insert.pointers = [node, right_node]
            
            node.parent = to_insert
            right_node.parent = to_insert
            
            return to_insert
        return None
        
    elif not node.leaf:
        inserted = False
        for i in range(len(node.keys)):
            if key < node.keys[i]:
                pos = i
                res = insert_helper(node.pointers[i], key, value)
                inserted = True
                break
        if not inserted:
            pos = len(node.pointers) - 1
            res = insert_helper(node.pointers[-1], key, value)
        if res == None:
            return None
        node.keys.insert(pos, res.keys[0])
        node.pointers[pos] = res.pointers[0]
        node.pointers.insert(pos+1, res.pointers[1])
        node.pointers[pos].parent = node
        node.pointers[pos+1].parent = node
        
        if len(node.keys) > node.max_keys:
            num_left = len(node.keys) // 2
            
            right_node = Node()
            right_node.leaf = False
            right_node.keys = node.keys[num_left+1:]
            right_node.pointers = node.pointers[num_left+1:]
            for pointer in right_node.pointers:
                pointer.parent = right_node
            
            to_insert = Node()
            to_insert.leaf = False
            to_insert.keys = [node.keys[num_left]]
            to_insert.pointers = [node, right_node]
            
            node.keys = node.keys[:num_left]
            node.pointers = node.pointers[:num_left+1]
            
            node.parent = to_insert
            right_node.parent = to_insert
            
            return to_insert
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
    
    update_parent_lb(left)
    update_parent_lb(right)

def update_parent_lb(node):
    for i in range(1, len(node.parent.pointers)):
        if node.parent.pointers[i] is node:
            node.parent.keys[i-1] = node.keys[0]

def leaf_merge(left, right):
#     print("leaf_merge")
    left.keys.extend(right.keys)
    left.pointers.pop()
    left.pointers.extend(right.pointers)
    remove_from_parent_next_pointer_and_key(left)
    update_parent_lb(left)

def remove_from_parent_next_pointer_and_key(node):
    for i in range(len(node.parent.pointers)-1):
        if node.parent.pointers[i] is node:
            node.parent.pointers.pop(i+1)
            return node.parent.keys.pop(i)
        
def remove_from_parent_prev_pointer_and_key(node):
    for i in range(1, len(node.parent.pointers)):
        if node.parent.pointers[i] is node:
            node.parent.pointers.pop(i-1)
            return node.parent.keys.pop(i-1)

def distribute(left, right):
#     print("distribute")
    parent = left.parent
    pivot_pos = None
    for i in range(len(parent.pointers)):
        if parent.pointers[i] is left:
            pivot_pos = i
    if pivot_pos == None or pivot_pos == len(parent.pointers) - 1:
        print("pivot_pos:", pivot_pos)
        raise Exception("If left has a right sibling, it must have a pivot_pos < len(parent.pointers) - 1")
    assert parent.pointers[pivot_pos+1] is right
    
    num_left = (len(right.keys) + len(left.keys) + 1) // 2
    num_right = (len(right.keys) + len(left.keys)) - num_left
    
    for _ in range(num_left - len(left.keys)):
        left.keys.append(parent.keys[pivot_pos])
        parent.keys[pivot_pos] = right.keys.pop(0)
        left.pointers.append(right.pointers.pop(0))
        left.pointers[-1].parent = left
#         left.keys[-1] = left.pointers[-1].keys[0]
    
    for _ in range(num_right - len(right.keys)):
        right.keys.insert(0, parent.keys[pivot_pos])
        parent.keys[pivot_pos] = left.keys.pop()
        right.pointers.insert(0, left.pointers.pop())
        right.pointers[0].parent = right

def validate_parent_helper(root):
    # asserts that for every non-leaf node, its children points to itself as a parent
    for p in root.pointers:
        if type(p) == Node:
            assert p.parent is root
            validate_parent_helper(p)
        else:
            return

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
    if type(root.pointers[0]) != Node:
        return root.keys[0]
    for i in range(len(root.pointers)-1):
        assert root.pointers[i].keys[0] < root.pointers[i+1].keys[0]
    for i in range(len(root.pointers)):
        if i > 0:
#             print("A:", root.keys[i-1])
#             print("B:", validate(root.pointers[i]))
            assert root.keys[i-1] == validate_helper(root.pointers[i])
        else:
            validate_helper(root.pointers[i])
    return validate_helper(root.pointers[0])

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
        return root.pointers[-1], 0
    else:
        # find the subtree to recursively call on

        for i in range(len(root.keys)):
            if key < root.keys[i]:
                return search_first_gte(root.pointers[i], key)
        return search_first_gte(root.pointers[-1], key)

def get_num_nodes_helper(root):
    if type(root.pointers[0]) != Node:
        return 1
    return 1 + sum(get_num_nodes_helper(root.pointers[i]) for i in range(len(root.pointers)))



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
        if lower == None:
            lower = ""
        if upper == None:
            upper = chr(255) * 12
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
            node = node.pointers[-1]
            pos = 0
        # this return is needed if the res includes the rightmost leaf node
        return res

    def _delete(self, key):
        delete_helper(self.root, key)
        if self.root.pointers[0] == None:
            print("tree is empty")
        if len(self.root.keys) == 0:
            print("root is empty, shrinking tree level")
            self.root.pointers[0].parent = None
            self.root = self.root.pointers[0]

    def insert(self, augmented_key, value):
        # augmented_key is str(average_rating) + tconst
        res = insert_helper(self.root, augmented_key, value)
        if res != None:
            self.root = res

    def search(self, key):
        return self._search_range(str(key), str(key + 0.1))

    def search_range(self, lower, upper):
        if lower != None:
            lower = str(lower)
        if upper != None:
            upper = str(upper+0.1)
        return self._search_range(lower, upper)

    def delete(self, key):
        to_delete = self._search_range(str(key), str(key+0.1), True)
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
                    if type(pointer) != Node:
                        break
                    nxt.append(pointer)
            print(" ".join(str(x) for x in to_print))
            print()
            cur = nxt
    
    def validate(self):
        validate_parent_helper(self.root)
        validate_helper(self.root)

    def get_num_nodes(self):
        return get_num_nodes_helper(self.root)

    def get_height(self):
        res = 0
        cur = self.root
        while type(cur) == Node:
            res += 1
            cur = cur.pointers[0]
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
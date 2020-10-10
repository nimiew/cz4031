import unittest

from structures import Block, Disk
from utils import *

class TestUtils(unittest.TestCase):
    
    def test_parse_data(self):
        data = parse_data()
        self.assertEqual(data[0], ["tt0000001", 5.6, 1645])
        self.assertEqual(data[-1], ["tt9916778", 7.3, 24])
    
    def test_string_conversion(self):
        string = ""
        bytes_ = convert_string_to_bytes(string, 10)
        self.assertEqual(len(bytes_), 10)
        self.assertEqual(convert_bytes_to_string(bytes_), "")

        string_9 = "123456789"
        bytes_9 = convert_string_to_bytes(string_9, 10)
        self.assertEqual(len(bytes_9), 10)
        self.assertEqual(convert_bytes_to_string(bytes_9), string_9)

        string_10 = "0123456789"
        bytes_10 = convert_string_to_bytes(string_10, 10)
        self.assertEqual(len(bytes_10), 10)
        self.assertEqual(convert_bytes_to_string(bytes_10), string_10)

        string_11 = "0123456789A"
        with self.assertRaises(Exception) as e:
            convert_string_to_bytes(string_11, 10)
        self.assertEqual(str(e.exception), "string length: 11 exceeds num_bytes: 10")

        string_non_ascii = "你好"
        with self.assertRaises(Exception) as e:
            convert_string_to_bytes(string_non_ascii, 10)
        self.assertEqual(str(e.exception), "byte value: 20320 invalid")

    def test_float_conversion(self):
        f = 1.0
        bytes_ = convert_float_to_bytes(f)
        self.assertEqual(len(bytes_), 4)
        self.assertEqual(convert_bytes_to_uint(bytes_[:2]), 1)
        self.assertEqual(convert_bytes_to_uint(bytes_[2:]), 0)
        self.assertEqual(convert_bytes_to_float(bytes_), 1.0)

        f = 65535.65535
        bytes_ = convert_float_to_bytes(f)
        self.assertEqual(len(bytes_), 4)
        self.assertEqual(convert_bytes_to_uint(bytes_[:2]), 65535)
        self.assertEqual(convert_bytes_to_uint(bytes_[2:]), 65535)
        self.assertEqual(convert_bytes_to_float(bytes_), 65535.65535)

        f = 0.1
        with self.assertRaises(Exception) as e:
            convert_float_to_bytes(f)
        self.assertEqual(str(e.exception), "float value must be >= 1.0")
        
        f = -23.1
        with self.assertRaises(Exception) as e:
            convert_float_to_bytes(f)
        self.assertEqual(str(e.exception), "float value must be >= 1.0")

        f = 65536.0
        with self.assertRaises(Exception) as e:
            convert_float_to_bytes(f)
        self.assertEqual(str(e.exception), "before_dot > 65535")
        
        f = 1.65536
        with self.assertRaises(Exception) as e:
            convert_float_to_bytes(f)
        self.assertEqual(str(e.exception), "after_dot > 65535")
    
    def test_record_conversion(self):
        # test with dataset first 10k rows
        data = parse_data()
        for record in data[:10000]:
            converted_back_record = convert_bytes_to_record(convert_record_to_bytes(record))
            for i in range(3):
                self.assertEqual(record[i], converted_back_record[i])
    
    def test_data_block_header_setter_and_getter(self):
        test_block = Block()
        set_data_block_header(test_block, 23)
        self.assertEqual(get_block_type(test_block), "data")
        self.assertEqual(get_data_block_header(test_block), (0, 23, 13, 18))

        record = ["abc", 2.5, 23]
        record_bytes = convert_record_to_bytes(record)
        insert_record_bytes(test_block, record_bytes)
        self.assertEqual(get_data_block_header(test_block), (0, 23, 31, 18))

    def test_index_block_header_setter_and_setter(self):
        test_block = Block()
        set_index_block_header(test_block, "root", 5, 9, 1)
        self.assertEqual(get_block_type(test_block), "root")
        self.assertEqual(get_index_block_header(test_block), (1, 5, 9, 1, 14))

        test_block = Block()
        set_index_block_header(test_block, "non-leaf", 7, 45, 11)
        self.assertEqual(get_block_type(test_block), "non-leaf")
        self.assertEqual(get_index_block_header(test_block), (2, 7, 45, 11, 14))

        test_block = Block()
        set_index_block_header(test_block, "leaf", 23, 75, 23)
        self.assertEqual(get_block_type(test_block), "leaf")
        self.assertEqual(get_index_block_header(test_block), (3, 23, 75, 23, 14))

    def test_insert_and_read_record_bytes(self):
        test_block = Block()
        set_data_block_header(test_block, 23)
        record = ["abc", 2.5, 23]
        record_bytes = convert_record_to_bytes(record)
        self.assertTrue(insert_record_bytes(test_block, record_bytes))
        # first record will be at position 13 as header has 13 bytes
        self.assertEqual(read_record_bytes(test_block, 13), record_bytes)
        # header should be unchanged
        self.assertEqual(get_block_type(test_block), "data")
        num_records_written_to_block = 1
        while insert_record_bytes(test_block, record_bytes) != -1:
            num_records_written_to_block += 1
        self.assertEqual(num_records_written_to_block, (len(test_block)-13) // 18)
        # TODO: write test for exceptions

    def test_delete_record_bytes(self):
        test_block = Block()
        set_data_block_header(test_block, 25)
        records = [
            ["a", 2.8, 44],
            ["b", 6.8, 34],
            ["c", 3.8, 84]
        ]
        for record in records:
            record_bytes = convert_record_to_bytes(record)
            insert_record_bytes(test_block, record_bytes)
        delete_record_bytes(test_block, 31) # delete second record
        self.assertEqual(convert_bytes_to_record(read_record_bytes(test_block, 13)), records[0])
        self.assertEqual(convert_bytes_to_record(read_record_bytes(test_block, 31)), ["", 0.0, 0])

    def test_serialize_and_deserialize_index_block(self):
        test_block = Block()
        set_index_block_header(test_block, "root", 5, 0)
        self.assertEqual(deserialize_index_block(test_block), ([(0, 0)], []))
        
        # note: pointer is (block_id, offset)
        pointers = [(4, 0), (5, 1), (6, 2), (7, 3)]
        keys = [(5.6, "tt0000001"), (6.6, "tt0000001"), (7.6, "tt0000001")]
        keys_pointers_bytes = serialize_ptrs_keys(pointers, keys)
        self.assertTrue(set_ptrs_keys_bytes(test_block, keys_pointers_bytes))
        self.assertEqual(deserialize_index_block(test_block), (pointers, keys))

        pointers = [(4, 0), (5, 1)]
        keys = [(7.6, "tt0000001")]
        keys_pointers_bytes = serialize_ptrs_keys(pointers, keys)
        set_ptrs_keys_bytes(test_block, keys_pointers_bytes)
        self.assertEqual(deserialize_index_block(test_block), (pointers, keys))
        # TODO: write test for exceptions
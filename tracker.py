import collections

class Tracker:
    track_counts = collections.defaultdict(int)
    track_set = collections.defaultdict(dict) # use dict instead of set because dict retains order in python 3.6+

    @classmethod
    def increment_count(cls, key):
        cls.track_counts[key] += 1

    @classmethod
    def reset_count(cls, key):
        cls.track_counts[key] = 0

    @classmethod
    def reset_all_count(cls):
        cls.track_counts = collections.defaultdict(int)

    @classmethod
    def add_to_set(cls, key, value):
        cls.track_set[key][value] = True
    
    @classmethod
    def reset_set(cls, key):
        cls.track_set[key] = {}
    
    @classmethod
    def reset_all_set(cls):
        cls.track_set = collections.defaultdict(dict)

    @classmethod
    def reset_all(cls):
        cls.track_counts = collections.defaultdict(int)
        cls.track_set = collections.defaultdict(dict)
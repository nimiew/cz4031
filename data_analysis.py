def main():
    with open("data.tsv") as f:
        lines = f.readlines()
    
    # columns
    print(f"columns: {lines[0].split()}")
    print(f"first row: {lines[1].split()}")

    max_tconst_len = -1
    max_average_rating = -1
    min_average_rating = float('inf')
    max_num_votes = -1
    min_num_votes = float("inf")

    for line in lines[1:]:
        tconst, average_rating, num_votes = line.split()
        max_tconst_len = max(max_tconst_len, len(tconst))
        max_average_rating = max(max_average_rating, float(average_rating))
        min_average_rating = min(min_average_rating, float(average_rating))
        max_num_votes = max(max_num_votes, int(num_votes))
        min_num_votes = min(min_num_votes, int(num_votes))
        
        _, after_dot = average_rating.split(".")
        assert len(after_dot) <= 2
        
        # just to check if we got any funny values
        assert float(average_rating) > 0
        assert int(num_votes) > 0

    print(f"Number of records: {len(lines[1:])}")
    print(f"Minimum average rating: {min_average_rating}")
    print(f"Maximum average rating: {max_average_rating}")
    print(f"Minimum number of votes: {min_num_votes}")
    print(f"Maximum number of votes: {max_num_votes}")
    print(f"Maximum tconst length: {max_tconst_len}")

if __name__ == "__main__":
    main()
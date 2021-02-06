from utils import CSVMemoryEfficientOperations, calc_avg_std
import time

start_exec = time.perf_counter()    

CSVMemoryEfficientOperations.sortByKey("movielens/ratings.csv", "ratings_sorted.csv", 1)
CSVMemoryEfficientOperations.cleanFile("movielens/tags.csv", "tags_cleaned.csv")
CSVMemoryEfficientOperations.sortByKey("tags_cleaned.csv", "tags_sorted.csv", 1)
CSVMemoryEfficientOperations.sortByKeyByGroup("tags_sorted.csv", "tags_group_sorted.csv", 3, 1)
CSVMemoryEfficientOperations.groupColumnsByKey("ratings_sorted.csv", "ratings_grouped.csv", [2], 1, calc_avg_std)
CSVMemoryEfficientOperations.groupColumnsByKey("tags_group_sorted.csv", "tags_grouped.csv", [2], 1, None)
CSVMemoryEfficientOperations.cleanFile("ratings_grouped.csv", "ratings_grouped_cleaned.csv")
CSVMemoryEfficientOperations.cleanFile("tags_grouped.csv", "tags_grouped_cleaned.csv")
options = [{"key": 0, "columns": [1, 2]}, {"key": 0, "columns": [1]}, {"key": 0, "columns": [1]}]
CSVMemoryEfficientOperations.joinByKey(["movielens/movies.csv", "ratings_grouped_cleaned.csv", "tags_grouped_cleaned.csv"], "movies_joined.csv", options)
CSVMemoryEfficientOperations.cleanFile("movies_joined.csv", "movies_joined_cleaned.csv")
CSVMemoryEfficientOperations.deleteFile("ratings_sorted.csv")
CSVMemoryEfficientOperations.deleteFile("tags_cleaned.csv")
CSVMemoryEfficientOperations.deleteFile("tags_sorted.csv")
CSVMemoryEfficientOperations.deleteFile("tags_group_sorted.csv")
CSVMemoryEfficientOperations.deleteFile("ratings_grouped.csv")
CSVMemoryEfficientOperations.deleteFile("tags_grouped.csv")
CSVMemoryEfficientOperations.deleteFile("ratings_grouped_cleaned.csv")
CSVMemoryEfficientOperations.deleteFile("tags_grouped_cleaned.csv")
CSVMemoryEfficientOperations.deleteFile( "movies_joined.csv")

stop_exec = time.perf_counter()    
print("TOTAL EXEC TIME", f"{stop_exec - start_exec:0.4f} seconds")
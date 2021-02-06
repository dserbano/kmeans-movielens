from utils import CSVMemoryEfficientOperations, jaccard_similarity, cosine_similarity
import random, time

def hierarchicalClustering(data:list, options:list, k:int):

    clusters = [{"centroid": row, "members" : [{"distance": 0, "payload": row}]} for row in data]

    for n in range(0, len(clusters) - k):
        closest_distance = float('inf')
        cluster_i = float('inf')
        cluster_j = float('inf')
        for i in range(0, len(clusters)):
            for j in range(0, len(clusters)):
                if i != j:
                    distance = 0
                    for option in options:
                        feature_value_1 = clusters[i]["centroid"][option["feature"]]
                        feature_value_2 = clusters[j]["centroid"][option["feature"]]
                        distance_f = option["distance_f"]
                        distance = distance + distance_f(feature_value_1, feature_value_2)*option["weight"]

                    if distance < closest_distance and distance != 0:
                        closest_distance = distance
                        cluster_i = clusters[i]
                        cluster_j = clusters[j]

        
        cluster_merged = {"centroid": None, "members": cluster_i["members"] + cluster_j["members"]}
        cluster_merged["centroid"] = findCentroid(cluster_merged["members"], options)
        
        clusters.remove(cluster_i)
        clusters.remove(cluster_j)
        clusters.append(cluster_merged)
        
    return clusters

def findCentroid(members:list, options:list):
    curr_distance = float('inf')
    centroid = 0
    for i in range(0, len(members)):
        distance = 0
        for j in range(0, len(members)):
            if i != j:
                for option in options:
                    feature_value_1 = members[i]["payload"][option["feature"]]
                    feature_value_2 = members[j]["payload"][option["feature"]]
                    distance_f = option["distance_f"]
                    distance = distance + (distance_f(feature_value_1, feature_value_2)*option["weight"])**2

        if distance < curr_distance:
            curr_distance = distance
            centroid = members[i]

    for i in range(0, len(members)):
         for option in options:
            feature_value_1 = centroid["payload"][option["feature"]]
            feature_value_2 = members[j]["payload"][option["feature"]]
            distance_f = option["distance_f"]
            members[i]["distance"] = distance_f(feature_value_1, feature_value_2)

    members.sort(key=lambda x : x["distance"])
    return centroid["payload"]

def findRepresentatives(clusters):

    representatives = []
    
    for i in range(0, len(clusters)):

        lower_bound = clusters[i]["members"][int(len(clusters[i]["members"])*(60/100))]["distance"]
        upper_bound = clusters[i]["members"][int(len(clusters[i]["members"])*(80/100))]["distance"]

        representatives_i = []
        for j in range(0, len(clusters[i]["members"])):
            if clusters[i]["members"][j]["distance"] >= lower_bound and clusters[i]["members"][j]["distance"] <= upper_bound:
                representatives_i.append(clusters[i]["members"][j])
        if len(representatives_i) > 7:
            representatives_i = random.sample(representatives_i, 7)

        clusters[i]["members"] = []
        representatives.append(representatives_i)

    return representatives

def associateToCluster(row:list, representatives:list, options:list):
    curr_distance = float('inf')
    curr_i = 0
    for i in range(0, len(representatives)):
        for j in range(0, len(representatives[i])):
            distance = 0
            for option in options:
                feature_value_1 = row[option["feature"]]
                feature_value_2 = representatives[i][j]["payload"][option["feature"]]
                distance_f = option["distance_f"]
                distance = distance + distance_f(feature_value_1, feature_value_2)*option["weight"]

            if distance < curr_distance:
                curr_distance = distance
                curr_i = i

    return [curr_i, curr_distance] + row

def CUREDiskBased(in_file:str, process_id:str, options:list, k:int, chunk_size:int):
    offsets = CSVMemoryEfficientOperations.getLinesOffsets(in_file, chunk_size)
    headings = CSVMemoryEfficientOperations.getHeadings(in_file)

    initial_load = CSVMemoryEfficientOperations.readChunk(in_file, 0, offsets, chunk_size, options)

    clusters = hierarchicalClustering(initial_load, options, k)
   
    representatives = findRepresentatives(clusters)

    chunk = initial_load
    del initial_load
    count = 0

    while True:
        rows_output = []
        for i in range(0, len(chunk)):
            line = associateToCluster(chunk[i], representatives, options)
            for option in options:
                lambda_f = option["encode"] if option["encode"] else lambda x: x
                line[option["feature"] + 2] = lambda_f(line[option["feature"] + 2])
            rows_output.append(line)

        CSVMemoryEfficientOperations.writeChunk(process_id + ".csv", count, rows_output, ["cluster", "distance"] + headings)
        count = count + 1
        chunk = CSVMemoryEfficientOperations.readChunk(in_file, count, offsets, chunk_size, options)
        if chunk == None or len(chunk) < 1:
            break

    CSVMemoryEfficientOperations.sortByKey(process_id + ".csv", process_id + "_sorted.csv", 0)
    CSVMemoryEfficientOperations.sortByKeyByGroup(process_id + "_sorted.csv", process_id + ".csv", 1, 0)
    CSVMemoryEfficientOperations.deleteFile(process_id + "_sorted.csv")

    representatives_output = []
    for i in range(0, len(representatives)):
        for j in range(0, len(representatives[i])):
            line = representatives[i][j]["payload"]
            for option in options:
                lambda_f = option["encode"] if option["encode"] else lambda x: x
                line[option["feature"]] = lambda_f(line[option["feature"]])

            representatives_output.append([i] + [representatives[i][j]["distance"]] + line)
    CSVMemoryEfficientOperations.writeChunk(process_id + "_representatives.csv", 0, representatives_output, ["cluster", "distance"] + headings)

    CSVMemoryEfficientOperations.sortByKey(process_id + "_representatives.csv", process_id + "_representatives_sorted.csv", 0)
    CSVMemoryEfficientOperations.sortByKeyByGroup(process_id + "_representatives_sorted.csv", process_id + "_representatives.csv", 1, 0)
    CSVMemoryEfficientOperations.deleteFile(process_id + "_representatives_sorted.csv")

    centroids_output = []
    for i in range(0, len(clusters)):
        line = clusters[i]["centroid"]
        centroids_output.append([i] + line)
    CSVMemoryEfficientOperations.writeChunk(process_id + "_centroids.csv", 0, centroids_output, ["cluster"] + headings)

    CSVMemoryEfficientOperations.sortByKey(process_id + "_centroids.csv", process_id + "_centroids_sorted.csv", 0)
    CSVMemoryEfficientOperations.deleteFile(process_id + "_centroids_sorted.csv")

    return 


start_exec = time.perf_counter()    

#a) d1: jaccard similarity based on the genres of the movies
d1 = [{"feature": 2, "decode": lambda x: x.split("|"), "encode": lambda x: "|".join(x), "weight": 1, "distance_f": jaccard_similarity}]
CUREDiskBased("movies_joined_cleaned.csv", "exercise1_a", d1, 8, 650)

#b) d2: jaccard similarity based on the tags of the movies
d2 = [{"feature": 4, "decode": lambda x: x.split("|"), "encode": lambda x: "|".join(x), "weight": 1, "distance_f": jaccard_similarity}]
CUREDiskBased("movies_joined_cleaned.csv", "exercise1_b", d2, 8, 650)

#c) d3: cosine similarity based on the ratings of the movies
d3 = [{"feature": 3, "decode": lambda x: x.split("|"), "encode": lambda x: "|".join(x), "weight": 1, "distance_f": cosine_similarity}]
CUREDiskBased("movies_joined_cleaned.csv", "exercise1_c", d3, 8, 650)

#d) d4 = 0.3*d1 + 0.25*d2 + 0.45*d3
d4 = [{"feature": 2, "decode": lambda x: x.split("|"), "encode": lambda x: "|".join(x), "weight": 0.3, "distance_f": jaccard_similarity}, {"feature": 3, "decode": lambda x: x.split("|"), "encode": lambda x: "|".join(x), "weight": 0.45, "distance_f": cosine_similarity}, {"feature": 4, "decode": lambda x: x.split("|"), "encode": lambda x: "|".join(x), "weight": 0.25, "distance_f": jaccard_similarity}]
CUREDiskBased("movies_joined_cleaned.csv", "exercise1_d", d4, 8, 650)


stop_exec = time.perf_counter()    
print("TOTAL EXEC TIME", f"{stop_exec - start_exec:0.4f} seconds")
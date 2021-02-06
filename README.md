## About

I implemented a disk-based k-means flavor for clustering the movies of the movielens dataset. The program takes as input the number of clusters k, the distance function to use and any input file(s) that it needs. Its output is a list of movies and the cluster they belong to. It also prints the centroids/clustroids. I implemented the following distance functions for the movies:

<ul>
<li>d1: Jaccard similarity based on the genres of the movies;</li>
<li>d2: Jaccard similarity based on the tags of the movies;</li>
<li>d3: Cosine similarity based on the ratings of the movies;</li>
<li>d4 = 0.3*d1 + 0.25*d2 + 0.45*d3.</li>
</ul>

I implemented a slightly different version of the CURE algorithm, which is a two-pass algorithm. The reason why I chose CURE is because it is already disk based and can generate clusters of any shape and form. 

In the first pass a random sample of points is picked in such a way that it can fit in the main memory (RAM) and these points are clustered optimally by grouping together the nearest points. I used an expensive bottom-up hierarchical clustering algorithm that iterates over the data. Initially each point starts in its own cluster, and pairs of clusters are merged together as the iteration progresses until a predetermined number of clusters is formed. Because the features are not in Euclidean space, custom distance functions are used, such as the Jaccard similarity function and the Cosine similarity function. In order to simplify the implementation of the k-means algorithms and at the same time improve the operational and space complexity I did some pre-processing on the original data set by applying sorting, grouping, MapReduce and merging operations ulti-mately generating a single file indexed by the movies ids with all the necessary data.

For example, I didn’t use the full vector with all the scores per movie to calculate the cosine similarity between them but I calculated beforehand their normal distribution (mean and variance). An additional interesting improvement would be to extract for every movie a list of distributions calculated over specific time intervals in order to keep into account trend factors.
The preprocessing is done by the “preprocessing.py” file using a combination of methods from the “CSVMemoryEfficientOperations” library, situated in the “utils.py” file. In this library I implemented efficient MapReduce operations by using some helper data structures that allow for random access on csvs files. A list of offsets in bytes is kept for every row, when needed, so that any line no matter the position can be read in O(1) time. This is similar to how databases use indexing and smart pointers to implement fast read/write transactions. This allowed to do sorting not on the actual list of elements kept in memory, but on their offsets. 

The “CSVMemoryEfficientOperations” library also has methods for reading data in chunks efficiently.
For the biggest csv (“ratings.csv”), that has 25 million lines, the list of offsets/pointers is only 230 Mb in size. The size of this data structure depends uniquely on the number of rows and not the number of columns. A csv with 40 Gb and many columns but still 25 million lines requires also 230 Mb in the RAM for the offsets. In some cases, I loaded in memory also the ids of the entities under analysis, but because the ids are only 4 bytes, they do not occupy much memory as well and actually their total size is almost the same as the offsets for 25 million lines.

In the second step of this process some representatives are picked for every cluster found in the previous step. Because the data is not in Euclidean space the representatives are picked not by the usual method but by selecting exactly all the points distant from the centroid between 60% and 70% over the total points inside that cluster and then selecting 10 random points from these as representatives.

In the second pass the whole data set is rescanned and every point in the data set is placed in the closest cluster. The decision is taken not by comparing the distances of each feature of the data points from the centroids, but the distances of each feature of the data points from the representatives of each cluster and assigning each data piece to the corresponding cluster for which this distance is the shortest. Surprisingly, even if the initial sample points are very modest, it can still capture clusters of every good quality. For an initial sample of 1000 movies, each with a long vector of feature it takes half an hour to perform the clustering operation over all the data.

## How to execute

+ Install Python;
+ How to run the files.py:
	* open terminal;
	* navigate to the directory of file.py;
	* type 'python file.py' in the terminal to run;
+ "preprocessing.py" is to be executed before "k-means.py" in order to create the necessary support files;
+ for the "k-means.py" the required inputs are to be specified programmatically by changing the parameters of the "CUREDiskBased" function and the parameters are: in_file (the input files), process_id (this is the name of the csv files where the results are written at the end), options (the features to be used with the corresponding weights and distance function), k (the number of clusters), chunk_size (the number of csv lines analyzed at a time). All cases are already encoded and will run in order when executing "k-means.py";
+ the execution of "preprocessing.py" should last half an hour. This time can be greatly optimized;
+ Make sure the "movielens" directory with the data is in the same directory as the executable files. Download "movielens" from the following link: "https://grouplens.org/datasets/movielens/25m/".


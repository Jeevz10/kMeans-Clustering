# kMeans-Clustering
This is the second assignment I did when taking the course -- Big Data for Data Science. This assignment rquired me to implement kMeans clustering in Spark without the use of MLib. The programming language used is Scala.  

### Problem Statement 

To cluster posts found on StackOverflow according to their scores and domains. 

#### Dataset 

![image](https://user-images.githubusercontent.com/35773953/103085179-e1208c00-461b-11eb-9b34-87a68a9693a3.png)
![image](https://user-images.githubusercontent.com/35773953/103085200-f72e4c80-461b-11eb-95af-8ae6093718e2.png)


### Solution Design 

1. Group the questions and answers together. 
2. Choose the highest score for each question. 
3. Design the vectors for clustering with score and domain. Vector = (Domain Spread of 50000 x Index of Domain , Highest Score from all answer for a particular question) 
4. Cluster the vectors using k means algorithm. 

#### Structure of kMeans Algorithm 

1. Choose k (random) data points (seeds) to be the initial centroids (cluster centers) 
2. Assign each data point to the closest centroid by computing the distances to centroids 
3. Re-compute the centroids using the current cluster memberships
4. If a convergence criterion is not met, repeat steps 2 and 3. 

### Learning outcomes 
1. Learnt to use Reliable Distributed Datasets (RDDs) and its special set of function - groupByKey, aggregateByKey, reduceByKey. 

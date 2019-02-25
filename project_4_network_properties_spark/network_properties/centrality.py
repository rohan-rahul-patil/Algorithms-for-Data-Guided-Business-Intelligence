from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from pyspark.sql.functions import explode

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def closeness(g):
	
	# Get list of vertices. We'll generate all the shortest paths at
	# once using this list.
	# YOUR CODE HERE
    vertices = g.vertices.flatMap(lambda x: x).collect()
    #print(vertices)

	# first get all the path lengths.
    path_length = g.shortestPaths(landmarks = vertices)

	# Break up the map and group by ID for summing
    path_length = path_length.select(functions.explode('distances'))

	# Sum by ID
    sum_path_length = path_length.groupBy('key').agg(functions.sum('value').alias('sum'))

	# Get the inverses and generate desired dataframe.
    closeness = sum_path_length.withColumn('sum', 1 / sum_path_length.sum).selectExpr('key as id', 'sum as closeness') 
    
    return closeness


print("Reading in graph for problem 2.")
graph = sc.parallelize([('A','B'),('A','C'),('A','D'),
	('B','A'),('B','C'),('B','D'),('B','E'),
	('C','A'),('C','B'),('C','D'),('C','F'),('C','H'),
	('D','A'),('D','B'),('D','C'),('D','E'),('D','F'),('D','G'),
	('E','B'),('E','D'),('E','F'),('E','G'),
	('F','C'),('F','D'),('F','E'),('F','G'),('F','H'),
	('G','D'),('G','E'),('G','F'),
	('H','C'),('H','F'),('H','I'),
	('I','H'),('I','J'),
	('J','I')])
	
e = sqlContext.createDataFrame(graph,['src','dst'])
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()
print("Generating GraphFrame.")
g = GraphFrame(v,e)

print("Calculating closeness.")
sorted_closeness = closeness(g).sort('closeness',ascending=False)
sorted_closeness.show()

print("Writing distribution to file centrality_out.csv")
sorted_closeness.toPandas().to_csv("centrality_out.csv")


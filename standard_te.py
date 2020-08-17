from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys

#Check if argument are set
if len(sys.argv) != 3:
	print("Usage: Graph <file>", file=sys.stderr)
	exit(-1)

#Define spark session variable
spark = SparkSession \
	.builder \
	.appName("Standard Triangle Enumeration") \
	.getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 32)

#Read file
sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile(str(sys.argv[1]))
parts = lines.map(lambda l: l.split(" "))
graph = parts.map(lambda p: Row(i=int(p[0]), j=int(p[1])))

#Load the other direction edges with undirected graphs
if (sys.argv[2]== "undirected"):
	ungraph = parts.map(lambda p: Row(i=int(p[1]), j=int(p[0])))
	graph = graph.union(ungraph)

#Triangle enumeration with SQL Query
schemaPeople = spark.createDataFrame(graph)
schemaPeople.createOrReplaceTempView("E_s")
triangles = spark.sql("SELECT E1.i AS v1, E1.j AS v2, E2.j AS v3 FROM E_s E1 JOIN E_s E2 ON E1.j=E2.i JOIN E_s E3 ON E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j ORDER BY v1,v2,v3")
print(triangles.count())

spark.stop()

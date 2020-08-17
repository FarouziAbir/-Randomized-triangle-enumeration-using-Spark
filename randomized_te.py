from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as f
from pyspark.sql import SQLContext
import sys

#Check if argument are set
if len(sys.argv) != 3:
        print("Usage: Graph <file>", file=sys.stderr)
        exit(-1)

#Define spark session variable
spark = SparkSession \
        .builder \
        .appName("Randomized Triangle Enumeration") \
        .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 8)
#Read file
sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile(str(sys.argv[1]),8)
parts = lines.map(lambda l: l.split(" "))
graph = parts.map(lambda p: Row(i=int(p[0]), j=int(p[1])))

#Load other direction edges for undirected graphs
if (sys.argv[2]== "undirected"):
        ungraph = parts.map(lambda p: Row(i=int(p[1]), j=int(p[0])))
        graph = graph.union(ungraph)

#Save the graph in columnar format as parquet file (to speed up the queries execution)
schemaGraph = spark.createDataFrame(graph)

schemaGraph.write.format("parquet").mode("overwrite").save("/home/limosadm/codes/partitions/E_s.parquet")

graph = spark.read.parquet("/home/limosadm/codes/partitions/E_s.parquet")

graph.createOrReplaceTempView("E_s")

#Assign triplet to machines
t = [(1,1,1,1),(2,1,1,2),(3,1,2,1),(4,1,2,2),(5,2,1,1),(6,2,1,2),(7,2,2,1),(8,2,2,2)]
rdd = sc.parallelize(t)
triplet = rdd.map(lambda x: Row(machine=int(x[0]), color1=int(x[1]), color2=int(x[2]), color3=int(x[3])))
Triplet = spark.createDataFrame(triplet)
Triplet.createOrReplaceTempView("Triplet")

#Assign colors to vertices randomly in unique manner
df = spark.sql("SELECT i FROM (SELECT DISTINCT i FROM E_s UNION SELECT DISTINCT j FROM E_s)V")
V_s = df.withColumn("color", round(rand()*(2-1)+1,0))
V_s = V_s.withColumn("color", V_s["color"].cast(IntegerType()))
V_s.createOrReplaceTempView("V_s")

#Send edges to proxies
E_s_proxy = spark.sql("SELECT Vi.color AS i_color, Vj.color AS j_color, E.i AS i, E.j AS j FROM E_s E JOIN V_s Vi ON E.i=Vi.i JOIN V_s Vj ON E.j=Vj.i")

E_s_proxy.write.format("parquet").mode("overwrite").save("/home/limosadm/codes/partitions/E_s_proxy.parquet")
E_s_proxy = spark.read.parquet("/home/limosadm/codes/partitions/E_s_proxy.parquet")

#Create E_s_local
E1 = E_s_proxy.alias("E").join(broadcast(Triplet.alias("edge1")), (col("E.i_color")==col("edge1.color1")) & (col("E.j_color")==col("edge1.color2")), how='inner')\
      .where("E.i<E.j").select('machine','i','j','i_color','j_color')

E2 = E_s_proxy.alias("E").join(broadcast(Triplet.alias("edge2")), (col("E.i_color")==col("edge2.color2")) & (col("E.j_color")==col("edge2.color3")), how='inner')\
      .where("E.i<E.j").select('machine','i','j','i_color','j_color')

E3 = E_s_proxy.alias("E").join(broadcast(Triplet.alias("edge3")), (col("E.i_color")==col("edge3.color3")) & (col("E.j_color")==col("edge3.color1")), how='inner')\
      .where("E.i>E.j").select('machine','i','j','i_color','j_color')

#Save E_s_local in parquet file partitioned by machine to speed up the local joins for triangle enumeration
E1.write.partitionBy("machine").format("parquet").mode("overwrite").save("/home/limosadm/codes/partitions/E1.parquet")
E2.write.partitionBy("machine").format("parquet").mode("overwrite").save("/home/limosadm/codes/partitions/E2.parquet")
E3.write.partitionBy("machine").format("parquet").mode("overwrite").save("/home/limosadm/codes/partitions/E3.parquet")

#Local triangle enumeration by machine 
sqlContext = SQLContext(sc)
#---------------- 1 ----------------
E1 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E1.parquet/machine=1")
E2 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E2.parquet/machine=1")
E3 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E3.parquet/machine=1")

triangle = E1.alias("E1").join(E2.alias("E2"), col("E1.j")==col("E2.i"), how="inner").join(E3.alias("E3"), col("E2.j")==col("E3.i"), how="inner")\
      .where("E1.i=E3.j").select(f.col('E1.i').alias('v1'), f.col('E1.j').alias('v2'), f.col('E2.j').alias('v3'))

count = triangle.count()
print(triangle.count())
#--------------- 2 ----------------
E1 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E1.parquet/machine=2")
E2 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E2.parquet/machine=2")
E3 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E3.parquet/machine=2")

triangle = E1.alias("E1").join(E2.alias("E2"), col("E1.j")==col("E2.i"), how="inner").join(E3.alias("E3"), col("E2.j")==col("E3.i"), how="inner")\
      .where("E1.i=E3.j").select(f.col('E1.i').alias('v1'), f.col('E1.j').alias('v2'), f.col('E2.j').alias('v3'))
count = count + triangle.count()
print(triangle.count())

#--------------- 3 ----------------
E1 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E1.parquet/machine=3")
E2 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E2.parquet/machine=3")
E3 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E3.parquet/machine=3")

triangle = E1.alias("E1").join(E2.alias("E2"), col("E1.j")==col("E2.i"), how="inner").join(E3.alias("E3"), col("E2.j")==col("E3.i"), how="inner")\
      .where("E1.i=E3.j").select(f.col('E1.i').alias('v1'), f.col('E1.j').alias('v2'), f.col('E2.j').alias('v3'))
count = count + triangle.count()
print(triangle.count())

#--------------- 4 ----------------
E1 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E1.parquet/machine=4")
E2 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E2.parquet/machine=4")
E3 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E3.parquet/machine=4")

triangle = E1.alias("E1").join(E2.alias("E2"), col("E1.j")==col("E2.i"), how="inner").join(E3.alias("E3"), col("E2.j")==col("E3.i"), how="inner")\
      .where("E1.i=E3.j").select(f.col('E1.i').alias('v1'), f.col('E1.j').alias('v2'), f.col('E2.j').alias('v3'))
count = count + triangle.count()
print(triangle.count())

#--------------- 5 ----------------
E1 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E1.parquet/machine=5")
E2 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E2.parquet/machine=5")
E3 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E3.parquet/machine=5")

triangle = E1.alias("E1").join(E2.alias("E2"), col("E1.j")==col("E2.i"), how="inner").join(E3.alias("E3"), col("E2.j")==col("E3.i"), how="inner")\
      .where("E1.i=E3.j").select(f.col('E1.i').alias('v1'), f.col('E1.j').alias('v2'), f.col('E2.j').alias('v3'))
count = count + triangle.count()
print(triangle.count())

#--------------- 6 ----------------
E1 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E1.parquet/machine=6")
E2 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E2.parquet/machine=6")
E3 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E3.parquet/machine=6")

triangle = E1.alias("E1").join(E2.alias("E2"), col("E1.j")==col("E2.i"), how="inner").join(E3.alias("E3"), col("E2.j")==col("E3.i"), how="inner")\
      .where("E1.i=E3.j").select(f.col('E1.i').alias('v1'), f.col('E1.j').alias('v2'), f.col('E2.j').alias('v3'))
count = count + triangle.count()
print(triangle.count())

#--------------- 7 ----------------
E1 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E1.parquet/machine=7")
E2 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E2.parquet/machine=7")
E3 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E3.parquet/machine=7")

triangle = E1.alias("E1").join(E2.alias("E2"), col("E1.j")==col("E2.i"), how="inner").join(E3.alias("E3"), col("E2.j")==col("E3.i"), how="inner")\
      .where("E1.i=E3.j").select(f.col('E1.i').alias('v1'), f.col('E1.j').alias('v2'), f.col('E2.j').alias('v3'))
count = count + triangle.count()
print(triangle.count())

#--------------- 8 ----------------
E1 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E1.parquet/machine=8")
E2 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E2.parquet/machine=8")
E3 = sqlContext.read.parquet("/home/limosadm/codes/partitions/E3.parquet/machine=8")

triangle = E1.alias("E1").join(E2.alias("E2"), col("E1.j")==col("E2.i"), how="inner").join(E3.alias("E3"), col("E2.j")==col("E3.i"), how="inner")\
      .where("E1.i=E3.j").select(f.col('E1.i').alias('v1'), f.col('E1.j').alias('v2'), f.col('E2.j').alias('v3'))
count = count + triangle.count()
print(triangle.count())


print(count)
spark.stop()


# Randomized triangle enumeration using Spark

This program allows to enumerate triangles in large graphs based on randomized algorithms using Spark SQL

# Data set to use

The data set to use need to be in the following manner:
    
v1 <- 1 space -> v2

For example:

1 2

3 2

5 6

.

.

etc

# Standard algorithm execution

In order to execute standard_te.py file, you need to specify the graph dataset path and orientation, whether it is directed or undireted, as presented in the following command

```
bin/spark-submit --master spark://your-master:7077 path/to/standard_te.py path/to/data_set directed/undirected
```

# Randomized algorithm execution

In order to execute randomized_te.py file,you need to specify the graph dataset path and orientation, whether it is directed or undireted, as presented in the following command

```
bin/spark-submit --master spark://your-master:7077 path/to/randomized_te.py path/to/data_set directed/undirected
```

import sys
from csv import reader
from pyspark import SparkContext


if __name__ == "__main__":
	if len(sys.argv) != 2:
		exit(-1)
	sc = SparkContext()
	lines = sc.textFile(','.join(sys.argv[1:]))
	result = lines.mapPartitions(lambda x: reader(x)) \
		.map(lambda x: (x[11], 1)) \
		.reduceByKey(lambda x, y: x + y) \
		.map(lambda (x, y):  x + '\t' + str(y))
	result.saveAsTextFile('group_levels.out')
	sc.stop()	



import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv) != 3:
		exit(-1)
	sc = SparkContext()
	col = int(sys.argv[1])
	lines = sc.textFile(','.join(sys.argv[2:]))
	result = lines.mapPartitions(lambda x: reader(x)) \
		.map(lambda x: (x[col], 1)) \
		.reduceByKey(lambda x, y: x + y) \
		.map(lambda (x, y):  x + '\t' + str(y))
	result.saveAsTextFile('result_' + sys.argv[1] + '.out')
	sc.stop()	



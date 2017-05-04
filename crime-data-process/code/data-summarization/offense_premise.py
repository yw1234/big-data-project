import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv) != 2:
		exit(-1)
	sc = SparkContext()
	lines = sc.textFile(','.join(sys.argv[1:]))
	result = lines.mapPartitions(lambda x: reader(x)) \
		.map(lambda x: (x[7] + ', ' + x[16], 1) if x[7] and 16 else ('outliers', 1)) \
		.filter(lambda (x, y): x != 'outliers') \
		.reduceByKey(lambda x, y: x + y) \
		.map(lambda (x, y):  x + '\t' + str(y))
	result.saveAsTextFile('result.out')
	sc.stop()	



import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv) != 2:
		exit(-1)
	sc = SparkContext()
	lines = sc.textFile(','.join(sys.argv[1:]))
	result = lines.mapPartitions(lambda x: reader(x)) \
		.map(lambda x: (x[0] + ' ' + x[1], int(x[5])) if x[0] and x[1] and int(x[1]) > 2005 else ('outliers', 1)) \
		.filter(lambda (x, y): x != 'outliers') \
		.reduceByKey(lambda x, y: x + y) \
		.map(lambda (x, y):  x + '\t' + str(y))
	result.saveAsTextFile('result.out')
	sc.stop()	



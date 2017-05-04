import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv) != 3:
		exit(-1)
	sc = SparkContext()
	lines = sc.textFile(','.join(sys.argv[2:]))
	result = lines.mapPartitions(lambda x: reader(x)) \
		.map(lambda x: (x[1], 1) if x[1] and x[13] and int(x[1].split('/')[2]) > 2005 and int(x[1].split('/')[2]) < 2016 else ('outliers', 1)) \
		.filter(lambda (x, y): x != 'outliers') \
		.reduceByKey(lambda x, y: x + y) \
		.map(lambda (x, y): (y, x)) \
		.sortByKey(False) \
		.map(lambda (x, y): (y, x)) \
		.map(lambda (x, y):  x + '\t' + str(y)) \
		.take(int(sys.argv[1]))
	sc.parallelize(result).saveAsTextFile('result.out')
	sc.stop()	



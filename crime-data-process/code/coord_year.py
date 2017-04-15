import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv) != 3:
		exit(-1)
	sc = SparkContext()
	year = sys.argv[1]
	lines = sc.textFile(','.join(sys.argv[2:]))
	result = lines.mapPartitions(lambda x: reader(x)) \
		.map(lambda x: (x[21] + ' ' + x[22], 1) if len(x) > 22 and x[21] and x[22] and x[1] and x[1].split('/')[2] == year else ('outliers', 1)) \
		.filter(lambda (x, y): x != 'outliers') \
		.reduceByKey(lambda x, y: x + y) \
		.map(lambda (x, y): x + ',')
	result.saveAsTextFile('result.out')
	sc.stop()	


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
		.map(lambda x: (x[13] + ' ' + x[1].split('/')[0] + ' ' + x[1].split('/')[2], 1) if x[1] and x[1].split('/')[2] == year else ('others', 1)) \
		.filter(lambda (x, y): x != 'others') \
		.reduceByKey(lambda x, y: x + y) \
		.map(lambda (x, y): x + '\t' + str(y))
	result.saveAsTextFile('result_' + year + '.out')
	sc.stop()	



import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv) != 2:
		exit(-1)
	sc = SparkContext()
	outside = ['STREET', 'BRIDGE', 'CEMETERY', 'MARINA/PIER', 'OPEN AREAS (OPEN LOTS)', 'PARK/PLAYGROUND']
	lines = sc.textFile(','.join(sys.argv[1:]))
	result = lines.mapPartitions(lambda x: reader(x)) \
		.map(lambda x: (x[1], 1) if x[16] and x[16] not in outside and x[1] and int(x[1].split('/')[2]) == 2011 else ('outliers', 1)) \
		.filter(lambda (x, y): x != 'outliers') \
		.reduceByKey(lambda x, y: x + y) \
                .sortByKey() \
		.map(lambda (x, y):  x + '\t' + str(y))
	result.saveAsTextFile('result.out')
	sc.stop()	



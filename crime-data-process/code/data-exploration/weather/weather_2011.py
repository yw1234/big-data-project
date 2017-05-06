import sys
from csv import reader
from pyspark import SparkContext

def formatYear(line):
    return line[1:5] + '/' + line[5:7] + '/' + line[7:9]    

def formatWeather(line):
    res = ""
    weather = ['Fog', 'Rain or Drizzle', 'Snow or Ice Pellets', 'Hail', 'Thunder', 'Tornado/Funnel Cloud']
    idx = 0;
    for c in line:
        if c == '1':
            res += weather[idx] + ';'
            idx += 1
    return res;
    

if __name__ == "__main__":
	if len(sys.argv) != 2:
		exit(-1)
	sc = SparkContext()
	lines = sc.textFile(','.join(sys.argv[1:]))
	result = lines.mapPartitions(lambda x: reader(x)) \
                .filter(lambda x: x[21].strip() != '000000' and x[2][1:5] == '2011') \
		.map(lambda x: (formatYear(x[2]), x[3].strip() + ', ' + formatWeather(x[21].strip())) if x[2] and x[3] and x[2] else ('outliers', 1)) \
		.filter(lambda (x, y): x != 'outliers') \
                .sortByKey() \
                .map(lambda (x, y): x + '\t' + y)
	result.saveAsTextFile('result.out')
	sc.stop()	



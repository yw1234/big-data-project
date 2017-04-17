## Project Goal

Transfer crime incidents’ address from raw text into geological information.

Create a heat map of crime in New York City.

Investigate the correlation between crime density and other features.

Google doc link: https://docs.google.com/document/d/1MoKjDjf2Nl-n7hrU6_UZD8Sj0PGedPebgnvanD5CA2M/edit?ts=58d45a49

## How to run

We use Spark as the data process tool, the dataset is put onto HDFS, the url is /user/wy609/nyc-crimes.csv. 

The data process code is under crime-data-process/code, and the generated results are under crime-data-process/results.

Run the source code file as the following command:

- spark-submit --num-executors {how many executors you want to use, usually I set 8} {url of your python file, like crime-data-process/code/level_year.py} /user/wy609/nyc-crimes.csv

The name of each source file is the {key1}_{key2}.py. For instance level_year.py, means that we use the the offense level and year as the key and get the count of how many crimes happened at the {key1} level in {key2} year.

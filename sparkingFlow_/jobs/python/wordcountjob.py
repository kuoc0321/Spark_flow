from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Pythonwordcount').getOrCreate()
text = 'kr8s is my kaiser Das fuhrer von Madrid'
words = spark.sparkContext.parallelize(text.split(' '))
wordcounts = words.map(lambda word : (word, 1)).reduceByKey(lambda a, b: a +b )
for wc in wordcounts.collect():
    print(wc[0], wc[1])
spark.stop()


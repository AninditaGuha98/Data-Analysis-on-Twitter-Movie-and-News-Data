from operator import add
from pyspark import SparkContext, SparkConf

conf = SparkConf("local").setAppName("FirstApp")
sc = SparkContext(conf=conf)
final_words = []

keywords = ['education', 'canada', 'university', 'dalhousie', 'expensive', 'good school', 'good schools', 'bad school',
            'bad schools', 'poor school', 'poor schools', 'faculty', 'computer science', 'graduate']
with open('news.json', 'r') as f:
    data_new = f.readlines()
    data = str(data_new).split(" ")
    for value in keywords:
        for i in range(len(data)):
            if (data[i].lower()) == value:
                final_words.append(value)

with open('twitter_data.json', 'r') as f1:
    data_new1 = f1.readlines()
    data = str(data_new1).split(" ")
    for value in keywords:
        for i in range(len(data)):
            if (data[i].lower()) == value:
                final_words.append(value)

tuple = (final_words)
my_dRDD = sc.parallelize(tuple)
word_count = my_dRDD.map(lambda y:
                      (y, 1)).reduceByKey(add).collect()
with open('output.txt','w') as f1:
    for l in word_count:
     f1.write(str(l))
print(word_count)

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)

def make_plot(counts):
    plt.plot([row[0][1] for row in counts])
    plt.plot([row[1][1] for row in counts])
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.show()
    plt.savefig('/usr/local/plot.png')


def load_wordlist(filename):
    #This function should return a list or set of words from the given filename.
    file = open(filename, 'r')
    allWords = set()
    for line in file:
        allWords.add(line[:-1])
    return allWords

def mapWord(w, pwords, nwords):
    if w in pwords:
        return ("positive", 1)
    else:
        return ("negative", 1)

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def stream(ssc, pwords, nwords, duration):

    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    words = tweets.flatMap(lambda line : line.split(" ")).filter(lambda word: word in pwords or word in nwords)   

    pairs = words.map(lambda w: mapWord(w, pwords, nwords))
    wCount = pairs.reduceByKey(lambda a,b: (a+b))

    runningCount = pairs.updateStateByKey(updateFunction)
    runningCount.pprint()
    
    counts = []
    wCount.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()

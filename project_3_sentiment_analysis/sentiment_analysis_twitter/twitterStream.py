import findspark
findspark.init()
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
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE

    positive_count = []
    negative_count = []
    
    for count in counts:
        for element in count:
            if element[0] == "positive":
                positive_count.append(element[1])
            else:
                negative_count.append(element[1])
                
    x = [i for i in range(0, len(positive_count))]
    
    positive = plt.plot(x, positive_count, '-b', label = "Positive")
    negative = plt.plot(x, negative_count, '-g', label = "Negative")
    plt.legend(loc = 'upper right')
    plt.xlabel("Time Step")
    plt.ylabel("Word Count")
    plt.show()
    plt.savefig('pos_neg_plot.png')

def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    f = open(filename, 'r')
    text = f.read()
    text = text.split('\n')
    return text
    
def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])
    
    words = tweets.flatMap(lambda x: x.split(" "))
    pos_neg_words = words.filter(lambda x: (x in pwords) or (x in nwords))
    pos_neg = pos_neg_words.map(lambda x: ("positive", 1) if x in pwords else("negative", 1))
    
    pos_neg_counts = pos_neg.reduceByKey(lambda x, y: x + y)
    total = pos_neg.updateStateByKey(updateFunction)
    total.pprint()

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    pos_neg_counts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)
    
    return counts


if __name__=="__main__":
    main()

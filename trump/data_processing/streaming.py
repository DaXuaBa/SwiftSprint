import re
import joblib

from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk import download
download('stopwords')
download('wordnet')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import configparser

config = configparser.ConfigParser()
config.read('./SwiftSprint/trump/data_processing/stream_app.conf')

kafka_host_name = config.get('kafka', 'host')
kafka_port_no = config.get('kafka', 'port_no')
input_kafka_topic_name = config.get('kafka', 'input_topic_name')
output_kafka_topic_name = config.get('kafka', 'output_topic_name')
kafka_bootstrap_servers = kafka_host_name + ':' + kafka_port_no

mysql_host_name = config.get('mysql', 'host')
mysql_port_no = config.get('mysql', 'port_no')
mysql_user_name = config.get('mysql', 'username')
mysql_password = config.get('mysql', 'password')
mysql_database_name = config.get('mysql', 'db_name')
mysql_driver = config.get('mysql', 'driver')

mysql_daxu_table_name = config.get('mysql', 'mysql_daxu_tbl')

mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name

db_properties = {}
db_properties['user'] = mysql_user_name
db_properties['password'] = mysql_password
db_properties['driver'] = mysql_driver

def save_to_mysql_table(current_df, epoc_id, mysql_table_name):

    mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + str(mysql_port_no) + "/" + mysql_database_name

    current_df = current_df.withColumn('batch_no', lit(epoc_id))

    current_df.write.jdbc(url = mysql_jdbc_url,
                  table = mysql_table_name,
                  mode = 'append',
                  properties = db_properties)

emojis = {':)': 'smile', ':-)': 'smile', ';d': 'wink', ':-E': 'vampire', ':(': 'sad', 
          ':-(': 'sad', ':-<': 'sad', ':P': 'raspberry', ':O': 'surprised',
          ':-@': 'shocked', ':@': 'shocked',':-$': 'confused', ':\\': 'annoyed', 
          ':#': 'mute', ':X': 'mute', ':^)': 'smile', ':-&': 'confused', '$_$': 'greedy',
          '@@': 'eyeroll', ':-!': 'confused', ':-D': 'smile', ':-0': 'yell', 'O.o': 'confused',
          '<(-_-)>': 'robot', 'd[-_-]b': 'dj', ":'-)": 'sadsmile', ';)': 'wink', 
          ';-)': 'wink', 'O:-)': 'angel','O*-)': 'angel','(:-D': 'gossip', '=^.^=': 'cat'}

mystopwordlist = ['a', 'about', 'above', 'after', 'again', 'ain', 'all', 'am', 'an',
             'and','any','are', 'as', 'at', 'be', 'because', 'been', 'before',
             'being', 'below', 'between','both', 'by', 'can', 'd', 'did', 'do',
             'does', 'doing', 'down', 'during', 'each','few', 'for', 'from', 
             'further', 'had', 'has', 'have', 'having', 'he', 'her', 'here',
             'hers', 'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in',
             'into','is', 'it', 'its', 'itself', 'just', 'll', 'm', 'ma',
             'me', 'more', 'most','my', 'myself', 'now', 'o', 'of', 'on', 'once',
             'only', 'or', 'other', 'our', 'ours','ourselves', 'out', 'own', 're',
             's', 'same', 'she', "shes", 'should', "shouldve",'so', 'some', 'such',
             't', 'than', 'that', "thatll", 'the', 'their', 'theirs', 'them',
             'themselves', 'then', 'there', 'these', 'they', 'this', 'those', 
             'through', 'to', 'too','under', 'until', 'up', 've', 'very', 'was',
             'we', 'were', 'what', 'when', 'where','which','while', 'who', 'whom',
             'why', 'will', 'with', 'won', 'y', 'you', "youd","youll", "youre",
             "youve", 'your', 'yours', 'yourself', 'yourselves']

stopwordlist = stopwords.words('english') + mystopwordlist

def preprocess(textdata):
    processedText = []
    wordLemma = WordNetLemmatizer()
    urlPattern        = r"((http://)[^ ]*|(https://)[^ ]*|( www\.)[^ ]*)" 
    userPattern       = '@[^\s]+' # e.g @FagbamigbeK check this out
    alphaPattern      = "[^a-zA-Z0-9]" # e.g I am *10 better!
    sequencePattern   = r"(.)\1\1+"  # e.g Heyyyyyyy, I am back!
    seqReplacePattern = r"\1\1" # e.g Replace Heyyyyyyy with Heyy
    
    for tweet in textdata:
        tweet = tweet.lower()
        # Replace all URls with 'URL'
        tweet = re.sub(urlPattern,' URL',tweet) 
        # Replace all emojis.
        for emoji in emojis.keys():
            tweet = tweet.replace(emoji, "EMOJI" + emojis[emoji])  
        # Replace @USERNAME to 'USER'.
        tweet = re.sub(userPattern,' USER', tweet)  
        # Replace all non alphabets.
        tweet = re.sub(alphaPattern, " ", tweet) # e.g I am *10 better!
        # Replace 3 or more consecutive letters by 2 letter.
        tweet = re.sub(sequencePattern, seqReplacePattern, tweet) # e.g Replace Heyyyyyyy with Heyy
         
        tweetwords = ''
        for word in tweet.split():
            if len(word) > 2 and word.isalpha():
                word = wordLemma.lemmatize(word)
                tweetwords += (word + ' ')
        processedText.append(tweetwords)
    return processedText

def load_model():
    vectoriser = joblib.load('./SwiftSprint/models/tfidf_vectoriser.pkl')
    LGBMmodel = joblib.load('./SwiftSprint/models/lgbm_model.pkl')
    return vectoriser, LGBMmodel

vectoriser, LGBMmodel = load_model()

def preprocess_text(text):
    processed_text = preprocess(text)
    return processed_text

def analyze_sentiment(text):
    if isinstance(text, str):
        text = [text]
    processed_text = preprocess_text(text)
    textdata = vectoriser.transform(processed_text)
    sentiment = LGBMmodel.predict(textdata)
    if len(sentiment) == 1:
        sentiment = [sentiment]
    return 1 if sentiment[0] == 1 else 0

if __name__ == "__main__":
    print("Real-Time Data Processing Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("SentimentAnalysis") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    tweet_schema = StructType() \
        .add("tweet", StringType()) \
        .add("state", StringType()) \
        .add("state_code", StringType())
    
    predict_udf = udf(lambda text: analyze_sentiment(text), IntegerType())

    tweet_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", input_kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    tweet_df = tweet_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), tweet_schema).alias("data")) \
        .select("data.*") \
        .withColumn("sentiment", predict_udf("tweet"))

    tweet_df1 = tweet_df.groupBy("state_code") \
        .agg(sum("sentiment").alias("total"))
    
    tweet_df2 = tweet_df1 \
        .withColumn("name", lit("trump"))
    tweet_df2.printSchema()

    tweet_df2 \
        .writeStream \
        .trigger(processingTime='30 seconds') \
        .outputMode("update") \
        .foreachBatch(lambda current_df, epoc_id: save_to_mysql_table(current_df, epoc_id, mysql_daxu_table_name)) \
        .start()
        
    tweet_df3 = tweet_df.groupBy("state") \
        .agg(sum("sentiment").alias("sum_sentiment")) \
    
    tweet_df4 = tweet_df3 \
        .join(tweet_df, tweet_df.state == tweet_df3.state, "inner") \
        .select("state", "state_code", "sum_sentiment", "timestamp", "user") \
        .withColumn("timestamp", date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn("user", lit("trump"))
    tweet_df4.printSchema()

    tweet_process_stream = tweet_df4 \
        .writeStream \
        .trigger(processingTime='30 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()
    
    kafka_writer_query = tweet_df4 \
        .selectExpr("user as key", "to_json(struct(*)) as value") \
        .writeStream \
        .trigger(processingTime='30 seconds') \
        .queryName("Kafka Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_kafka_topic_name) \
        .outputMode("update") \
        .option("checkpointLocation", "kafka-check-point-dir") \
        .start()
    
    tweet_process_stream.awaitTermination()
    kafka_writer_query.awaitTermination()

    print("Real-Time Data Processing Application Completed.")
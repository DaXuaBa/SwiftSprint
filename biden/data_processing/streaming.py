import re
import joblib

from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk import download
download('stopwords')
download('wordnet')

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from configparser import ConfigParser

conf_file_path = "./lgbm/data_processing/"
conf_file_name = conf_file_path + "stream_app.conf"
config_obj = ConfigParser()
config_read_obj = config_obj.read(conf_file_name)

kafka_host_name = config_obj.get('kafka', 'host')
kafka_port_no = config_obj.get('kafka', 'port_no')
input_kafka_topic_name = config_obj.get('kafka', 'input_topic_name')
output_kafka_topic_name = config_obj.get('kafka', 'output_topic_name')
kafka_bootstrap_servers = kafka_host_name + ':' + kafka_port_no

mysql_host_name = config_obj.get('mysql', 'host')
mysql_port_no = config_obj.get('mysql', 'port_no')
mysql_user_name = config_obj.get('mysql', 'username')
mysql_password = config_obj.get('mysql', 'password')
mysql_database_name = config_obj.get('mysql', 'db_name')
mysql_driver = config_obj.get('mysql', 'driver')

mysql_byname_table_name = config_obj.get('mysql', 'mysql_byname_tbl')
mysql_bygender_table_name = config_obj.get('mysql', 'mysql_bygender_tbl')
mysql_bymajors_table_name = config_obj.get('mysql', 'mysql_bymajors_tbl')
mysql_bydepartment_table_name = config_obj.get('mysql', 'mysql_bydepartment_tbl')
mysql_byyearstudy_table_name = config_obj.get('mysql', 'mysql_byyearstudy_tbl')

mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name

db_properties = {}
db_properties['user'] = mysql_user_name
db_properties['password'] = mysql_password
db_properties['driver'] = mysql_driver

def save_to_mysql_table(current_df, epoc_id, mysql_table_name):

    print("Printing MySQL table name: " + mysql_table_name)

    mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + str(mysql_port_no) + "/" + mysql_database_name

    current_df = current_df.withColumn('batch_no', lit(epoc_id))

    current_df.write.jdbc(url = mysql_jdbc_url,
                  table = mysql_table_name,
                  mode = 'append',
                  properties = db_properties)

    print("Exit out of save to MySQL table function")

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
    processed_text = preprocess_text(text)
    textdata = vectoriser.transform(processed_text)
    sentiment = LGBMmodel.predict(textdata)
    return '1' if sentiment[0] == 1 else '0'

if __name__ == "__main__":
    print("Real-Time Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("SentimentAnalysis") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    tweet_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", input_kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()
    print("Printing Schema from Apache Kafka: ")
    tweet_df.printSchema()

    tweet_df1 = tweet_df.selectExpr("CAST(value AS STRING)", "timestamp")

    tweet_schema = StructType() \
        .add("created_at", StringType()) \
        .add("tweet_id", StringType()) \
        .add("tweet", StringType())

    tweet_df2 = tweet_df1\
        .select(from_json(col("value"), tweet_schema)\
        .alias("data"), "timestamp")

    tweet_df3 = tweet_df2.select("data.*", "timestamp")

    tweet_df3 = tweet_df3.withColumn("partition_date", to_date("created_at"))
    tweet_df3 = tweet_df3.withColumn("partition_hour", hour(to_timestamp("created_at", 'yyyy-MM-dd HH:mm:ss')))

    tweet_agg_write_stream_pre = tweet_df3 \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
    
    print("Printing Schema of Bronze Layer: ")
    tweet_df3.printSchema()

    predict_udf = udf(lambda text: analyze_sentiment(text), StringType())

        # Áp dụng hàm dự đoán cho cột "tweet" trong DataFrame
    tweet_df4 = tweet_df3.withColumn("sentiment", predict_udf("tweet"))

    # Print the schema of the DataFrame
    print("Printing Schema of Sentiment: ")
    tweet_df4.printSchema()

    # Ghi DataFrame kết quả ra console
    tweet_agg_write_stream = tweet_df4 \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    tweet_agg_write_stream.awaitTermination()

    print("Real-Time Data Processing Application Completed.")
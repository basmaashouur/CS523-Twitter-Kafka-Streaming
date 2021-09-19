# CS523-Twitter-Kafka-Streaming

An app streaming tweets from twitter using kafka based on some hashtag and then make Sentiment Analysis on them and save them on a hive table

- Compatible with java **7+**.


## Authors:
- Basma Ashour
- Saloni Vora
- Shristi Maharjan 

[![Build Badge](https://travis-ci.org/k4m4/kickthemout.svg?branch=master)](https://github.com/basmaashouur/CS523-Twitter-Kafka-Streaming)
[![Compatibility](https://img.shields.io/badge/java-brightgreen.svg)](https://github.com/basmaashouur/reading-tracker)

---

## CentOS Installation

1.  Start the kafka server
```
~ ❯❯❯ cd kafka-<VERSION>

~/kafka-<VERSION> ❯❯❯ bin/zookeeper-server-start.sh config/zookeeper.properties

~/kafka-<VERSION> ❯❯❯ bin/kafka-server-start.sh config/server.properties
```
2. Create a hive table

```
~ ❯❯❯ hive
~ hive> drop table TwitterData;

~ hive> CREATE EXTERNAL TABLE IF NOT EXISTS TwitterData(createdAt STRING,Id STRING,userId STRING,location STRING,followersCount STRING,isVerified STRING,UserCreatedAt STRING,timezone STRING,sentiment STRING,tweetHours STRING,tweetMinutes STRING,tweetSeconds STRING,userCreatedMonth STRING,userCreatedYear STRING,hashtags STRING,userName STRING,Text STRING)COMMENT 'Twitter Live Data'ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",","quoteChar" = "\"" )LOCATION '/user/cloudera/twitterTweets';
```
3. Ddownload CS523-Twitter-Kafka-Streaming by cloning the [Git Repo](https://github.com/basmaashouur/CS523-Twitter-Kafka-Streaming)
4. Import the project using eclipse 
5. Run `KafkaProducer, KafkaStreamSQL, SentimentAnalyzer`



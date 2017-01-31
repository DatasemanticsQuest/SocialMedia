#initializes the logging service
import logging
logging.basicConfig(filename='search_api.log',level=logging.DEBUG,format='%(asctime)s %(message)s')

import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import argparse
import json
import datetime

import threading, logging, time
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from datetime import datetime

import string
import twitter_utils
import simplejson as json
from time import gmtime, strftime

from confluent.schemaregistry.client import CachedSchemaRegistryClient
from confluent.schemaregistry.serializers import MessageSerializer, Util
from confluent_kafka import Producer

import MySQLdb

topicname='twitter_streaming_quest_final'
cmdargs = str(sys.argv[1])

## THIS PROGRAM USES TWEEPY TO SEARCH TWITTER FOR HISTORICAL TWEETS AND WORKS BUT MAXES OUT ON NUMBER OF TWEETS RETURNED
## THIS TRIES TO MAKE EFFECTIVE USE OF MAX_ID

def createPayload(tweet):
#creates a payload based on the tweet
    logging.debug(datetime.now().strftime("%A, %d. %B %Y %I:%M%p"))
    payload = []
#    print '\n author id: ' + str(tweet.author.id_str)
    try:
        payload.append(str(tweet.author.id_str))
    except Exception,e:
        logging.debug('Warning in id_str : %s' % e)
        print 'Warning in id_str : ',e
        payload.append('NULL')

#    print '\n screen_name'+str(tweet.author.screen_name)
    try:
        payload.append(twitter_utils.checkNull(tweet.author.screen_name))
    except Exception,e:
        logging.debug('Warning in screen_name : %s' % e)
        print 'Warning in screen_name : ',e
        payload.append('NULL')

#    print '\n description'
    try:
        payload.append(twitter_utils.checkNull(tweet.author.description))
    except Exception,e:
        logging.debug('Warning in description : %s' % e)
        print 'Warning in description : ',e
        payload.append('NULL')

#    print '\n favourites_count'
    try:
        payload.append(str(tweet.author.favourites_count))
    except Exception,e:
        logging.debug('Warning in favourites_count : %s' % e)
        print 'Warning in favourites_count : ',e
        payload.append('0')

#    print '\n followers_count'
    try:
        payload.append(str(tweet.author.followers_count))
    except Exception,e:
        logging.debug('Warning in followers_count : %s' % e)
        print 'Warning in followers_count : ',e
        payload.append('0')

#    print '\n friends_count'
    try:
        payload.append(str(tweet.author.friends_count))
    except Exception,e:
        logging.debug('Warning in friends_count : %s' % e)
        print 'Warning in friends_count : ',e
        payload.append('0')

#    print '\n listed_count'
    try:
        payload.append(str(tweet.author.listed_count))
    except Exception,e:
        logging.debug('Warning in listed_count : %s' % e)
        print 'Warning in listed_count : ',e
        payload.append('0')

#    print '\n location'
    try:
        payload.append(twitter_utils.checkNull(tweet.author.location))
    except Exception,e:
        logging.debug('Warning in location : %s' % e)
        print 'Warning in location : ',e
        payload.append('NULL')

#    print '\n id_str'
    try:
        payload.append(str(tweet.author.id_str))
    except Exception,e:
        logging.debug('Warning in id_str : %s' % e)
        print 'Warning in id_str : ',e
        payload.append('NULL')

#    print '\n time_zone'
    try:
        payload.append(twitter_utils.checkNull(tweet.author.time_zone))
    except Exception,e:
        logging.debug('Warning in time zone : %s' % e)
        print 'Warning in : time_zone ',e
        payload.append('NULL')

#    print '\n statuses_count'
    try:
        payload.append(str(tweet.author.statuses_count))
    except Exception,e:
        logging.debug('Warning in statuses_count : %s' % e)
        print 'Warning in statuses_count : ',e
        payload.append('0')

#    print '\n created_at'
    try:
        payload.append(str(tweet.created_at.strftime('%Y-%m-%d  %H:%M:%S')))
    except Exception,e:
        logging.debug('Warning in created_at : %s' % e)
        print 'Warning in created_at : ',e
        payload.append('NULL')

#    print '\n favorite_count'
    try:
        payload.append(twitter_utils.checkNull(str(tweet.favorite_count)))
    except Exception,e:
        logging.debug('Warning in favourite_count : %s' % e)
        print 'Warning in favourite_count : ',e
        payload.append('0')

#    print '\n id_str'
    try:
        payload.append(str(tweet.id_str))
    except Exception,e:
        logging.debug('Warning in id_str : %s' % e)
        print 'Warning in id_str : ',e
        payload.append('NULL')

#    print '\n in_reply_to_status_id_str'
    try:
        payload.append(twitter_utils.checkNull(str(tweet.in_reply_to_status_id_str)))
    except Exception,e:
        logging.debug('Warning in in_reply_to_status_id_str : %s' % e)
        print 'Warning in in_reply_to_status_id_str : ',e
        payload.append('NULL')

#    print '\n in_reply_to_user_id_str'
    try:
        payload.append(twitter_utils.checkNull(str(tweet.in_reply_to_user_id_str)))
    except Exception,e:
        logging.debug('Warning in in_reply_to_user_id_str : %s' % e)
        print 'Warning in in_reply_to_user_id_str : ',e
        payload.append('NULL')

#    print '\n lang'
    try:
        language=twitter_utils.checkNull(str(tweet.lang))
        payload.append(language)
    except Exception,e:
        logging.debug('Warning in lang : %s' % e)
        print 'Warning in lang : ',e
        payload.append('NULL')

#    print '\n possibly_sensitive'
    try:
        payload.append(twitter_utils.checkNull(tweet.possibly_sensitive))
    except Exception,e:
        logging.debug('Warning in possibly_sensitive : %s' % e)
        print 'Warning in possibly_sensitive: ',e
        payload.append('NULL')

#    print '\n retweet_count'
    try:
        payload.append(str(tweet.retweet_count))
    except Exception,e:
        logging.debug('Warning in retweet count : %s' % e)
        print 'Warning in retweet_count : ',e
        payload.append('NULL')

#    print '\n text'
    try:
        tweet_text=twitter_utils.checkNull(tweet.text.lower())
        payload.append(tweet_text.lower())
    except Exception,e:
        logging.debug('Warning in text : %s' % e)
        print 'Warning in text : ',e
        payload.append('NULL')
        tweet_text='error'

#    print '\n url0'
    try:
        payload.append(twitter_utils.checkNull(str(tweet.entities["urls"][0]["url"])))
    except Exception,e:
        logging.debug('Warning in url : %s' % e)
        print 'Warning in url :',e
        payload.append('NULL')

#    print '\n url1'
    try:
        payload.append(twitter_utils.checkNull(str(tweet.entities["urls"][0]["expanded_url"])))
    except Exception,e:
        logging.debug('Warning in expanded_url : %s' % e)
        print 'Warning in expanded_url : ',e
        payload.append('NULL')

#    print '\n url2'
    try:
        payload.append(twitter_utils.checkNull(str(tweet.entities["media"][0]["media_url"])))
    except Exception,e:
        logging.debug('Warning in media_url : %s' % e)
        print 'Warning in media_url : ',e
        payload.append('NULL')

#sentiment analysis
#appends a list of values to the payload to indicate the strength of individual sentiments
    sentiment = twitter_utils.getSentiment(tweet_text, language)
    try:
        payload.append(str(sentiment['disgust']))
    except Exception,e:
        logging.debug('Warning in sentiment analysis (disgust) : %s' % e)
        print 'Warning in sentiment analysis : ',e
        payload.append(0)
    try:
        payload.append(str(sentiment['fear']))
    except Exception,e:
        logging.debug('Warning in sentiment analysis (fear) : %s' % e)
        print 'Warning in sentiment analysis : ',e
        payload.append(0)
    try:
        payload.append(str(sentiment['sadness']))
    except Exception,e:
        logging.debug('Warning in sentiment analysis (sadness) : %s' % e)
        print 'Warning in sentiment analysis : ',e
        payload.append(0)
    try:
        payload.append(str(sentiment['surprise']))
    except Exception,e:
        logging.debug('Warning in sentiment analysis (surprise) : %s' % e)
        print 'Warning in sentiment analysis : ',e
        payload.append(0)
    try:
        payload.append(str(sentiment['trust']))
    except Exception,e:
        logging.debug('Warning in sentiment analysis (trust) : %s' % e)
        print 'Warning in sentiment analysis : ',e
        payload.append(0)
    try:
        payload.append(str(sentiment['negative']))
    except Exception,e:
        logging.debug('Warning in sentiment analysis (negative) : %s' % e)
        print 'Warning in sentiment analysis : ',e
        payload.append(0)
    try:
        payload.append(str(sentiment['positive']))
    except Exception,e:
        logging.debug('Warning in sentiment analysis (positive) : %s' % e)
        print 'Warning in sentiment analysis : ',e
        payload.append(0)
    try:
        payload.append(str(sentiment['neutral']))
    except Exception,e:
        logging.debug('Warning in sentiment analysis (neutral) : %s' % e)
        print 'Warning in sentiment analysis : ',e
        payload.append(0)

#category text mining
#tags the payload with celebrity, accessory, event and brand, based on values in the sql db
    index_list=twitter_utils.load_inxlist()
    catlist=twitter_utils.tweet_category(tweet_text,index_list)
    try:
        payload.append(catlist[0])
    except Exception,e:
        logging.debug('Warning in text mining : %s' % e)
        print 'Warning in text mining : ',e
        payload.append(0)
    try:
        payload.append(catlist[1])
    except Exception,e:
        logging.debug('Warning in text mining : %s' % e)
        print 'Warning in text mining : ',e
        payload.append(0)
    try:
        payload.append(catlist[2])
    except Exception,e:
        logging.debug('Warning in text mining : %s' % e)
        print 'Warning in text mining : ',e
        payload.append(0)
    try:
        payload.append(catlist[3])
    except Exception,e:
        logging.debug('Warning in text mining : %s' % e)
        print 'Warning in text mining : ',e
        payload.append(0)

    return(payload)


def convert_long(val):
#converts the value to long
    try:
        converted=long(val)
    except Exception,e:
        logging.debug('Error in long type conversion inside the creation of the avro schema:  %s' % e)
        print("Error in Long type conversion : ",e)
        twitter_utils.sendErrorMail('There was an error in long type conversion inside the avro schema creation. The error is %s.' % e)
        converted=0
    return converted

def writeToavro(p,mls):
#converts the payload into the avro format in preparation for loading into hbase
        try:
            avro_schema = Util.parse_schema_from_string(open('/root/quest/twitter_avro_schema.avsc').read())
            client = CachedSchemaRegistryClient(url='http://192.168.111.12:8081')
            schema_id = client.register('twitter_avro__schema_stream4', avro_schema)
            avro_schema = client.get_by_id(schema_id)
            schema_id,avro_schema,schema_version = client.get_latest_schema('twitter_avro__schema_stream4')
            schema_version = client.get_version('twitter_avro__schema_stream4', avro_schema)
            serializer = MessageSerializer(client)
            encoded = serializer.encode_record_with_schema(topicname, avro_schema, {"authid": mls[0],
                                "screen_name": mls[1],
                                "description": mls[2],
                                "favourites_count": convert_long(mls[3]),
                                "followers_count": convert_long(mls[4]),
                                "friends_count": convert_long(mls[5]),
                                "listed_count": convert_long(mls[6]),
                                "location": mls[7],
                                "id_str": mls[8],
                                "time_zone": mls[9],
                                "statuses_count": convert_long(mls[10]),
                                "created_at": mls[11],
                                "favorite_count": convert_long(mls[12]),
                                "tid": mls[13],
                                "in_reply_to_status_id_str": mls[14],
                                "in_reply_to_user_id_str": mls[15],
                                "lang": mls[16],
                                "possibly_sensitive": mls[17],
                                "retweet_count": convert_long(mls[18]),
                                "text": mls[19],
                                "entities_url": mls[20],
                                "entities_expanded_url": mls[21],
                                "entities_media_url": mls[22],
                                "disgust": convert_long(mls[23]),
                                "fear": convert_long(mls[24]),
                                "sadness": convert_long(mls[25]),
                                "surprise": convert_long(mls[26]),
                                "trust": convert_long(mls[27]),
                                "negative": convert_long(mls[28]),
                                "positive": convert_long(mls[29]),
                                "neutral": convert_long(mls[30]),
                                "celebrities": (mls[31]),
                                "events": (mls[32]),
                                "brands": (mls[33]),
                                "accessories": (mls[34])
                                })
        except Exception,e:
                logging.debug('There was an error in the generation of the avro file. The error is: %s' % e)
                print 'Error in avro generation : ',e
                print mls
                twitter_utils.sendErrorMail('There was an error in the generation of the avro file. The error is %s' % e)
                return True
        try:
            p.produce(topicname, encoded)	    
            print 'succesfully added to topic ='+str(topicname)+' at: '+str(datetime.now())
        except Exception,e:
            logging.debug('There was an error in writing to the Kafka topic. The error is: %s' % e)
            print 'Error in  writing in kafka topic'
            twitter_utils.sendErrorMail('There was an error in writing to the kafka topic. The error is %s' % e)
            return True	

if __name__ == '__main__':
	try:
	    conf = {'bootstrap.servers': 'localhost:9092'}
            p = Producer(**conf)
	except Exception,e:
            print 'Error in Producer Configuration : ',e

        db = MySQLdb.connect("192.168.111.10","root","","quest_streaming" )
        cursor = db.cursor()

#gets credentials for twitter from the sql db
        sql = "select * from quest_streaming.app_details where app_id = 19;"
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                consumer_key = str(row[1])
                consumer_secret = str(row[2])
                access_token = str(row[3])
                access_secret = str(row[4])
        except Exception,e:
            logging.debug('Problem in credentials retrieval: %s' % e)
            print 'Problem in credentials retrieval: %s' % e
            twitter_utils.sendErrorMail('There was an issue in retrieving the credentials from the database. The error is %s.' % e)

	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_secret)
	api = tweepy.API(auth)

#creates a cartesian product of the celebrity names with the accessories for the purposes of creating a search string
        celebrity_track = []
        accessory_track = []
        search_text = []

	celebrity_track.append(cmdargs)
	
        sql = "select name from quest_streaming.trends where type='accessory';"
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                accessory_track.append(row[0])
        except Exception,e:
            logging.debug('Problem in accessory retrieval: %s' % e)
            print 'Problem in accessory retrieval: ',e
            twitter_utils.sendErrorMail('There was an issue in retrieving the list of accessories from the database. The error is %s.' % e)

        for accessory in accessory_track:
            for celebrity in celebrity_track:
                search_text.append("%s %s" % (celebrity,accessory))

        cursor.close()
        db.close()
	print search_text
        logging.debug('The search string is: %s' % ', '.join(search_text))

	last_id = -1
	searched_tweets = []
	max_tweets = 1000
	tot_tweets = 0
	tot_tweets_item = 0
	api_calls = 0
	
#Uses the search string, one item at a time, to retrieve all relevant tweets
	for i in range(len(search_text)):
		if i > 0:
			print ' Total tweets for ' + search_text[i].split(' ')[2] + ' : ' + str(tot_tweets_item)
			tot_tweets_item = 0
		while len(searched_tweets) < max_tweets:

			print('Processing the string: ' + search_text[i])
			count = max_tweets - len(searched_tweets)

			try:
				new_tweets = api.search(q=search_text[i],count=count,max_id=str(last_id - 1))
				if not new_tweets:
					print 'Tweets not found - trying again!'
					break
				searched_tweets.extend(new_tweets)
				last_id = new_tweets[-1].id

			except tweepy.TweepError as e:
				time.sleep(15 * 60)
		k=0
		for tweets in searched_tweets:
		    k=k+1
		    try:
#			print search_text[i],' have totally ',str(len(searched_tweets)),' and current pos is : ',k
			mls=createPayload(tweets)
			print mls
			writeToavro(p,mls)
		    except Exception,e:
            		logging.debug('Error in writing into Kafka: %s' % e)
            		print 'Error in writing into Kafka: %s' % e
            		twitter_utils.sendErrorMail('There was an issue in writing into Kafka. The error is %s.' % e)
			
			print 'Error in Writing into Kafka : ',e

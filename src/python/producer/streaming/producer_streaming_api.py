#-*- coding: utf-8 -*-
#!/usr/bin/env python

#initializes the logging service
import logging
import sys
reload(sys)
sys.setdefaultencoding('utf8')

logging.basicConfig(filename='quest_%d.log' % int(sys.argv[1]),level=logging.DEBUG,format='%(asctime)s %(message)s')

import tweepy
import threading, logging, time
from datetime import datetime

import string
import twitter_utils 
import simplejson as json
from time import gmtime, strftime, sleep
import pytz
import MySQLdb
import master_producer_utils

from confluent.schemaregistry.client import CachedSchemaRegistryClient
from confluent.schemaregistry.serializers import MessageSerializer, Util
from confluent_kafka import Producer

topicname='twitter_streaming_**'
cmdargs = int(sys.argv[1])

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


######################################################################
#Create a handler for the streaming data that stays open...
######################################################################

class StdOutListener(tweepy.StreamListener):
    def on_status(self, message):
        logging.debug(datetime.now().strftime("%A, %d. %B %Y %I:%M%p"))
        try:
	    mls= createPayload(message)
	    print mls
            timevar = datetime.utcnow() - datetime.strptime(mls[11],'%Y-%m-%d %H:%M:%S')
	    print datetime.utcnow(),datetime.strptime(mls[11],'%Y-%m-%d %H:%M:%S')
            print "Minutes and Seconds : ", divmod(timevar.days * 86400 + timevar.seconds, 60)
	except Exception,e:
            logging.debug('There was an error in creating the payload. The error is: %s' % e)
            print 'Error in Payload Creation : ',str(e)
	    twitter_utils.sendErrorMail('There was an error in creating the payload. The error is %s' % e)
            return True

        try:
#converts the payload into the avro format in preparation for loading into hbase
	    avro_schema = Util.parse_schema_from_string(open('/**/**/twitter.avsc').read())
	    client = CachedSchemaRegistryClient(url='http://192.168.**:8081')
	    schema_id = client.register('twitter_avro_schema_stream4', avro_schema)
	    avro_schema = client.get_by_id(schema_id)
	    schema_id,avro_schema,schema_version = client.get_latest_schema('twitter_avro_schema_stream4')
	    schema_version = client.get_version('twitter_avro_schema_stream4', avro_schema)
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
		twitter_utils.sendErrorMail('There was an error in the generation of the avro file. The error is %s. This is likely due to an error in the schema. Please check the schema file under twitter_avro_schema.avsc' % e)
        	return True
        
	try:
	    p.produce(topicname, encoded)
	    print 'succesfully added to topic ='+str(topicname)+' at: '+str(datetime.now())
        except Exception,e:
            logging.debug('There was an error in writing to the Kafka topic. The error is: %s' % e)
            print 'Error in  writing in kafka topic'
	    twitter_utils.sendErrorMail('There was an error in writing to the kafka topic. The error is %s' % e)
            return True

    def on_error(self, status_code):
#error handling, most importantly in the case of 420 errors
        logging.debug(datetime.now().strftime("%A, %d. %B %Y %I:%M%p"))
        logging.debug('There was a generic error. The status code is %d. The producer number is %d.' % (status_code,cmdargs))
        print('Got an error with status code: ' + str(status_code))
        if status_code == 420:
            producer_change_result = master_producer_utils.change_prod_credentials(cmdargs)
            if producer_change_result == 'producer found':
                print "Producer found"
                logging.debug("A 420 error was encountered but an app which was not at capacity was found and switched to.")
		sys.exit()
            else:
            	print "Producer not found"
	    	twitter_utils.sendErrorMail('A 420 error was encountered and the stored proc either failed to execute or no available producer was found. Intervention is required. The producer number is %d' % cmdargs)
	    	logging.debug('A 420 error was encountered and the stored proc either failed to execute or no available producer was found. Intervention is required. The producer number is %d' % cmdargs)
            	sleep(2*60*60)
	return True # To continue listening

    def on_timeout(self):
#timeout issue handling
        logging.debug(datetime.now().strftime("%A, %d. %B %Y %I:%M%p"))
	logging.debug('There was a timeout error from the the Twitter Listener. The producer number is %d.' % cmdargs)
        print('Timeout...')
	twitter_utils.sendErrorMail('There was a timeout error from the the Twitter Listener. The producer number is %d.' % cmdargs)
        return True # To continue listening

######################################################################
#Main Loop Init
######################################################################


if __name__ == '__main__':
    try:
        conf = {'bootstrap.servers': 'localhost:9092'}
        p = Producer(**conf)
	listener = StdOutListener()

        db = MySQLdb.connect("192.168.*.*","root","","quest_stream" )
        cursor = db.cursor()

#gets credentials for twitter from the sql db
	sql = "select app_id from quest_streaming.producer_details where producer_id = %d;" % cmdargs
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                app_id = int(row[0])
        except Exception,e:
            logging.debug('Problem in app id retrieval: %s' % e)
	    print 'Problem in app id retrieval: %s' % e
	    twitter_utils.sendErrorMail('There was an issue in retrieving the application id from the database. The error is %s.' % e)
	
    	sql = "select * from quest_streaming.app_details where app_id = %d;" % app_id
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
	
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token,access_secret)
        stream = tweepy.Stream(auth, listener)

#creates a cartesian product of the celebrity names with the accessories for the purposes of creating a search string
        celebrity_track = []
        accessory_track = []
	track_data = []

        sql = "select name from quest_streaming.trends where id in (select celebrity_id from quest_streaming.celebrity_producer where producer_id = %d);" % cmdargs
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
		celebrity_track.append(row[0])
        except Exception,e:
	    logging.debug('Problem in celebrity retrieval: %s' % e)
            print 'Problem in celebrity retrieval: ',e
            twitter_utils.sendErrorMail('There was an issue in retrieving the celebrity details from the database. The error is %s.' % e)

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
		track_data.append("%s %s" % (celebrity,accessory))

        cursor.close()
        db.close()
	print track_data
        logging.debug('The search string is: %s' % ', '.join(track_data))

#Uses the search string to retrieve all relevant tweets
	stream.filter(track=track_data, languages=['en'])
    except Exception,e:
	logging.debug('There was an error in the main loop of the producer. The error is: %s. The producer number is %d' % (e,cmdargs))
	print "Error in main loop"
	print Exception,e
        twitter_utils.sendErrorMail('There was an issue in the main loop of the producer. The error is %s. The producer number is %d' % (e,cmdargs))

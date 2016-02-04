'''import pymongo

def mongo_connect():
    try:
        conn = pymongo.MongoClient()  #connecting database
        print "Mongo is connected!"
        return conn
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoB: %s" % e

conn = mongo_connect()
db = conn['twitter_stream']
coll = db.my_collection
docs = [{'name': 'Code', 'surmane': 'Institute', 'twitter':'@coderinstitute'},
            {'name': 'John', 'surmane': 'Smith', 'twitter':'@smithy'}]
coll.insert_many(docs)
result = coll.find_one()
print result
    #print db.collection_names()
    #print conn.database_names()



mongo_connect()'''

'''import pymongo
import datetime
from datetime import timedelta


def mongo_connect():
    try:
        conn = pymongo.MongoClient()
        print "Mongo is connected!"
        return conn
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoB: %s" % e


conn = mongo_connect()
db = conn['twitter_stream']
coll = db.my_collection
coll.drop()
docs = [{'name': 'Code', 'surmane': 'Institute', 'twitter': '@coderinstitute',
         'date': datetime.datetime.utcnow()},
        {'name': 'John', 'surmane': 'Smith', 'twitter': '@smithy',
         'date': datetime.datetime.utcnow() - timedelta(days=2)},
        {'name': 'BOB', 'surmane': 'Paulson', 'twitter': '@smithy',
         'date': datetime.datetime.utcnow() + timedelta(days=10)},
        {'name': 'Larry', 'surmane': 'King', 'twitter': '@King09',
         'date': datetime.datetime.utcnow() - timedelta(days=10), '_id': '22'}]
coll.insert_many(docs)  # insert is similar to append
# results = coll.find({'name': 'John'})
# print results.count()
# print coll.count()

date = datetime.datetime.utcnow()
for doc in coll.find({"date": {"$lte": date}}).sort("name"):
    print doc
# mongo_connect()'''


import json
import pymongo
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener



CONSUMER_KEY = 'nRl43wXJIJuUB8pjJRCEgaxhK'
CONSUMER_SECRET = 'b3gOvBgdMLRO2DoIaFQviqScTYyffAvhjm3ekjNAEOOf1rCP3r'
OAUTH_TOKEN = '588663819-xhhPknNBD2UJqDCoy7fpxZzyqr6Zp1QpHjKoVGCX'
OAUTH_TOKEN_SECRET = '5uQl4RxtNkLGeHQdUIsKGClxlVtLThRzeIyvVydnKbsRP'

keyword_list = ['christmas']


class MyStreamListener(StreamListener):
    def __init__(self):
        self.num_tweets = 0  # cpunter starting at 0
        self.tweet_coll = None  # signal that something will go into tweet_coll at some stage

    def mongo_connect(self):
        try:
            client = pymongo.MongoClient()
            print "Mongo is connected!"
            db = client.tech_tweetsDB
            self.tweet_coll = db.tweets
        except pymongo.errors.ConnectionFailure, e:
            print "Could not connect to MongoB: %s" % e

    def on_data(self, data):
        try:
            # read in a tweet
            status = json.loads(data)
            print json.dumps(status, indent=4)
            tweet = {}
            tweet["text"] = status['text'].encode('utf8')  # autf able to read other languages
            tweet['screen_name'] = status['user']['screen_name']
            tweet['followers_count'] = status['user']['followers_count']
            tweet['friends_count'] = status['user']['friends_count']
            tweet['favorite_count'] = status['favorite_count']
            tweet['retweet_count'] = status['retweet_count']
            tweet['created at'] = status['created_at']
            print status.get('entities').get("media")
            if status.get('entities').get("media"):
                print status.get('entities').get("media")
                media = status['entities']["media"]
                tweet['media'] = media[0]["display_url"]
            else:
                tweet['media'] = None

            tweet['lang'] = status['user']['lang']
            tweet['location'] = status['user']['location']

            self.num_tweets += 1
            print self.num_tweets
            if self.num_tweets < 5:
                # Insert tweet in to the collection
                self.tweet_coll.insert(tweet)

                return True
            else:
                return False

            return True
        except BaseException as e:
            print('Failed on_data: %s' % str(e))
        return True

    def on_error(self, status):
        print(status)
        return '\n'.join(json_docs)


auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
# api = tweepy.API(auth)
stream = MyStreamListener()
stream.mongo_connect()

twitter_stream = Stream(auth, stream)
twitter_stream.filter(track=keyword_list)

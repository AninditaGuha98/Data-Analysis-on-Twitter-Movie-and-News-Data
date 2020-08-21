author = "Anindita"

import tweepy
import re
import pprint
import json
import requests
import pymongo

consumer_key = ""
consumer_secret_key = ""
access_key = ""
access_token_secret = ""

auth = tweepy.OAuthHandler(consumer_key, consumer_secret_key)
auth.set_access_token(access_key, access_token_secret)
api = tweepy.API(auth)

mongo = pymongo.MongoClient("54.89.151.125:27017")
db = mongo['Data_Assignment']
col_twitter = db['TwitterData']
col_movies = db['MoviesData']
col_news = db['NewsData']

keywords_twitter = ["Canada", "University", "Dalhousie University", "Halifax", "Canada Education"]

# Function for cleaning data
def clean_tweets(tweets):
    tw = re.sub("(https?|http)://[-a-zA-Z0-9+&@#/%?=~_|!:;]*[-a-zA-Z+&@#/%=~_|]", '', str(tweets))
    return str(tw).encode('ascii', 'ignore').decode('ascii')


# Extracting twitter data
def fetch_tweets(count):
    insert_json = []
    for i in range(8):
        for val in keywords_twitter:
            fetched_tweets = api.search(val, count=count)
            for status in fetched_tweets:
                json_data = status._json
                dict_tweets = {
                    "created_at": json_data['created_at'],
                    "tweet_id": json_data['id'],
                    "text": clean_tweets(json_data['text']),
                    "user_id": json_data['user']['id'],
                    "user_name": clean_tweets(json_data['user']['name']),
                    "user_location": clean_tweets(json_data['user']['location']),
                    "followers_count": json_data['user']['followers_count'],
                    "retweet_count": json_data['retweet_count']
                }
                try:
                    dict_tweets["retweet_text"] = clean_tweets(json_data['retweeted_status']['text'])
                except KeyError:
                    pass
                insert_json.append(dict_tweets)
    with open("twitter_data.json", 'w') as f:
        json.dump(insert_json, f, indent=4)
    f.close()
    #     Sending data to twitter collection of MongoDB database in EC2 instance.
    with open('twitter_data.json', 'r') as fr:
        data = json.load(fr)
        col_twitter.insert_many(data)


# Extracting news data
keywords_news = ["Canada", "University", "Dalhousie University", "Halifax", "Canada Education", "Moncton", "Toronto"]


def extract_news():
    final_dict = []
    for val in range(10):
        for words in keywords_news:
            url = ('http://newsapi.org/v2/everything?'
                   'q=' + words + '&'
                                  'from=2020-03-27&'
                                  'sortBy=popularity&'
                                  'apiKey=')
            response = requests.get(url)
            to_clean = response.json()
            for i in range(len(to_clean['articles'])):
                to_clean['articles'][i]['author'] = clean_tweets(to_clean['articles'][i]['author'])
                to_clean['articles'][i]['title'] = clean_tweets(to_clean['articles'][i]['title'])
                to_clean['articles'][i]['description'] = clean_tweets(to_clean['articles'][i]['description'])
                to_clean['articles'][i]['content'] = clean_tweets(to_clean['articles'][i]['content'])
            final_dict.append(to_clean)
    with open('news.json', 'w') as f1:
        json.dump(final_dict, f1, indent=4)
    f1.close()
    #     Sending data to news collection of MongoDB database in EC2 instance.
    with open('news.json', 'r') as fr:
        data = json.load(fr)
        col_news.insert_many(data)



# Extracting movie data
keywords_movie = ["Canada", "University", "Vancouver", "Halifax", "Canada Education", "Moncton", "Toronto", "Alberta",
                  "Niagara"]
def movie_data():
    movie_list = []
    for words in keywords_movie:
        url1 = ('http://www.omdbapi.com/?s=' + words + '&apikey=')
        response = requests.get(url1)
        to_clean = response.json()
        try:
            for i in range(len(to_clean['Search'])):
                titleSearch= to_clean['Search'][i]['Title']
                url2=('http://www.omdbapi.com/?t=' + titleSearch + '&apikey=')
                response2=requests.get(url2)
                to_clean2=response2.json()
                dict_movies={
                    'Title': to_clean2['Title'],
                    'Released':to_clean2['Released'],
                    'Genre': to_clean2['Genre'],
                    'Plot': to_clean2['Plot'],
                    'Rating': to_clean2['imdbRating']
                }
                movie_list.append(dict_movies)
        except KeyError:
            pass
    with open('movies.json', 'w') as f2:
        json.dump(movie_list, f2, indent=4)
    #     Sending data to movies collection of MongoDB database in EC2 instance.
    with open('movies.json', 'r') as fr:
        data = json.load(fr)
        col_movies.insert_many(data)


# Function calls

movie_data()
extract_news()
fetch_tweets(100)

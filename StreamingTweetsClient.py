import socket
import sys
import requests
import requests_oauthlib
import json


ACCESS_TOKEN = "884824522175660032-1m6cV7GwddpwJ84c5YGc4o3sxJdIGLf"
ACCESS_TOKEN_SECRET = "Odp4tCiwie58KjMBHjhly2eLFHyRhYeceQ7FTZmcEZhaP"
CONSUMER_KEY = "v1DeRY5WJyloSXqERCxG6lIHJ"
CONSUMER_SECRET = "nkFtERfl6ufDrNyC23voqJdOabcSl7pXJSHpLCjUVjgqDw8Yqu"

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET)


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:

            full_tweet = json.loads(line).get('entities').get('hashtags')
            for obj in full_tweet:
                # print(obj.get('text'))
                # hash_file = open("extractedtags.txt", 'a')
                # hash_file.write(obj.get('text') + '\n')
                tcp_connection.send(bytes(obj.get('text') + "\n", 'utf-8'))
            # tweet_text = full_tweet['text']
            # print("Tweet Text: " + tweet_text)
            # print("------------------------------------------")
            # tweet_data = bytes(tweet_text + "\n", 'utf-8')
            # tcp_connection.send(tweet_data)
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    #query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
    query_data = [('locations', '-130,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    # print(query_url, response)
    return response


TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp,conn)

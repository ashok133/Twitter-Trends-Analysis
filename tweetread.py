import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import datetime
import csv

consumer_key = '###'
consumer_secret = '###'
access_token = '###'
access_secret = '###'

tweets_per_min = {}

class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads( data )
          # print (len(msg['text'].encode('utf-8')))
          if 'trump' in msg['text'].encode('utf-8'):
            print "Trump found!"
            now = datetime.datetime.now()
            if now.minute in tweets_per_min:
              tweets_per_min.update({now.minute:tweets_per_min[now.minute]+1})
            else:
              tweets_per_min.update({now.minute:1})
            print tweets_per_min
            with open('tweet_count2.csv', 'wb') as csv_file:
              writer = csv.writer(csv_file)
              for key, value in tweets_per_min.items():
                writer.writerow([key, value])

          self.client_socket.send( msg['text'].encode('utf-8') )
          return True
      except BaseException as e:
          print ("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket,term):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)

  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=[term])

if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "192.168.0.104"      # Get local machine name
  port = 5555                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port
  term = raw_input("What would you like to search about?")

  print ("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print ("Received request from: " + str(addr))
  sendData(c,term)

import pandas as pd
import requests #Pushshift accesses Reddit via an url so this is needed
import json #JSON manipulation
import csv #To Convert final table into a csv file to save to your machine
import time
from datetime import datetime
import argparse

# Global dictionary to store the collected data
data_dict = {}

def getPostPushshiftData(subreddit, query, after, before, size):
  '''
  Build the URL used to scrape 'size' submissions/posts from a specific 'subreddit'
  filtered by a 'query' during a period of time ('before' and 'after')
  '''
  
  #Build PushShift URL to request data
  url = 'https://api.pushshift.io/reddit/search/submission/?q='+str(query)+'&size='+str(size)+'&after='+str(after)+'&before='+str(before)+'&subreddit='+str(subreddit)

  print("Requesting data from URL: \n", url)
  #Request URL
  r = requests.get(url)
  #Load JSON data from webpage into data variable
  data = json.loads(r.text)
  #return the data element which contains all the submissions/posts data
  return data['data']

def collectSubData(subm):
  '''
  Extract fields of interest from each post, store results in 'data_dict'
  '''

  global data_dict

  data_dict['post_id'].append(subm['id'])
  data_dict['title'].append(subm['title'])
  data_dict['author'].append(subm['author'])
  data_dict['text'].append(subm['selftext'])
  t = subm['created_utc']
  d = stampToDateObj(t)
  data_dict['stamp'].append(t)
  data_dict['date'].append(str(d.date()))
  data_dict['time'].append(str(d.time()))
  data_dict['score'].append(subm['score'])
  data_dict['comm'].append(subm['num_comments'])
  data_dict['url'].append(subm['permalink'])
  try:
      remove_reason = subm['removed_by_category']
  except KeyError:
      remove_reason = ""
  data_dict['remove_reason'] = remove_reason


def stampToDateObj(t):
  return datetime.fromtimestamp(t)


def dateToStamp(d):
  return datetime.timestamp(d)


def initDataDict():
  '''
  Initilize the variable which stores the collected data
  * This is where you modify what data to be collected
  '''
  fields_of_interest = ['post_id', 'title', 'author', 'text', 'date', 'time', 'query', 'score', 'comm', 'url', 'stamp', 'remove_reason']
  for f in fields_of_interest:
    data_dict[f] = []


def startScraping(sub, query, after, before, filename, size=10): # max:500
  '''
  Starting point of data scraping
  '''
  initDataDict()

  while True:
    data = getPostPushshiftData(sub, query, after, before, size)
    if len(data) == 0:
      print("empty data - no response")
      break
    for post in data:
      collectSubData(post)
    # update the starting date based on retrieved data
    after = data[-1]['created_utc']
    # wait 1 seconds, abiding Reddit rules (https://github.com/reddit-archive/reddit/wiki/API#rules)
    time.sleep(1)
    break
  
  # Show statistics
  statistics(sub, query)

  # Fill in missing columns
  fillMissingColumns(query)

  # Save results 
  saveResult(filename)


def fillMissingColumns(query):
  '''
  To reduce processing time, the script for now look for a single query in a 
  single subreddit, no need to keep saving them in earlier stages. 
  '''
  # Since multiple queries will be used for this project, the field is added
  # Other fields can be added this way (for now) like subreddit name and its ID
  data_dict['query'] = [query] * len(data_dict['post_id'])


def statistics(sub, query):
  '''
  Prints basic statistics of collected data
  '''
  print("*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-")

  print("Session data statistics: \n")
  print("Posts scraped: {}".format(len(data_dict['post_id'])))
  print("From: {}".format(str(stampToDateObj(data_dict['stamp'][0]))))
  print("Until: {}".format(str(stampToDateObj(data_dict['stamp'][-1]))))
  print("Subreddit: {}".format(sub))
  print("Filtered by: \"{}\"".format(query))

  print("*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-")


def saveResult(filename):
  '''
  Saves the collected data, in form of dictionary, to a csv file
  '''

  print("Save results in file: ", filename)
  df = pd.DataFrame(data_dict)
  df.to_csv(filename)


if __name__ == '__main__':
  # Construct the argument parser
  ap = argparse.ArgumentParser()

  ap.add_argument("-r", "--subreddit", required=True, help="Subreddit Name")
  ap.add_argument("-q", "--query", required=True, help="Search term")
  #ap.add_argument("-s", "--size", required=True, type=int, help="Number of retrieved items")
  ap.add_argument("-a", "--after", required=True, help="Starting date, format: MM-DD-YYYY")
  ap.add_argument("-b", "--before", required=True, help="End date, format: MM-DD-YYYY")
  ap.add_argument("-f", "--filename", required=True, help="File name to store results")

  args = ap.parse_args()
  date_format = "%m-%d-%Y %H:%M:%S"

  # Validate the starting date
  try:
    after = datetime.strptime(args.after + " 00:00:00", date_format)
  except ValueError:
    print("-a/--after is a date, should be formatted as: MM-DD-YYYY")
    exit()
  
  # Validate the end date
  try:
    before = datetime.strptime(args.before + " 23:59:59", date_format)
  except ValueError:
    print("-b/--before is a date, should be formatted as: MM-DD-YYYY")
    exit()
  
  # Validate time period
  if after > before:
    exit("Incorrect time period, '-a' indicates the start of the desired period, \
    where '-b' indicates the end. ")
  
  after = int(dateToStamp(after))
  before = int(dateToStamp(before))

  print("Arguments parsed .. Start scraping data \n\n")
  startScraping(args.subreddit, args.query, after, before, args.filename)


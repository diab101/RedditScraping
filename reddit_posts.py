import pandas as pd
import requests #Pushshift accesses Reddit via an url so this is needed
import json #JSON manipulation
import csv #To Convert final table into a csv file to save to your machine
import time
from datetime import datetime
import argparse

# Global dictionary to store the collected data
data_dict = {}


def stampToDateObj(t):
  return datetime.fromtimestamp(t)


def dateToStamp(d):
  return datetime.timestamp(d)


def initDataDict(query):
  '''
  Initilize the variable which stores the collected data
  * This is where you modify what data to be collected
  '''
  fields_of_interest = ['post_id', 'title', 'author', 'text', 'date', 'time', 'score', 'comm', 'url', 'stamp', 'remove_reason', 'subreddit']
  if query is not None:
    fields_of_interest.append('query')
  for f in fields_of_interest:
    data_dict[f] = []


def getPostPushshiftData(subreddit, after, before, query, size):
  '''
  Build the URL used to scrape 'size' submissions/posts from a specific 'subreddit'
  filtered by an optional 'query' during a period of time ('before' and 'after')
  '''
  
  #Build PushShift URL to request data
  if query is not None:
    url = 'https://api.pushshift.io/reddit/search/submission/?q='+str(query)+'&size='+str(size)+'&after='+str(after)+'&before='+str(before)+'&subreddit='+str(subreddit)
  else:
    url = 'https://api.pushshift.io/reddit/search/submission/?size='+str(size)+'&after='+str(after)+'&before='+str(before)+'&subreddit='+str(subreddit)

  #print("Requesting data from URL: \n", url)
  #Request URL
  while True:
    try:
      r = requests.get(url)
    except (requests.exceptions.ReadTimeout, requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError):
      print("PushShift timeout, site is down. waiting for 5 seconds and trying again")
      time.sleep(5)
      continue
    #Load JSON data from webpage into data variable
    try:
      data = json.loads(r.text)
    except json.decoder.JSONDecodeError:
      print("Decoding error, might be due to down server. Waiting 5 seconds and trying again")
      time.sleep(5)
      continue
    #return the data element which contains all the submissions/posts data
    return data['data']


def collectSubData(subm):
  '''
  Extract fields of interest from each post, store results in 'data_dict'
  '''

  global data_dict

  # clear previous data, just in case
  id, title, auth, text, t, score, com, link, remove_reason = " " * 9

  try: # to avoid script crashing 
    id = subm['id']
    title = subm['title']
    auth = subm['author']
    try:
      text = subm['selftext']
    except KeyError:
      text = "[banned]"
    t = subm['created_utc']
    score = subm['score']
    com = subm['num_comments']
    link = subm['permalink']
    try: # 'banned_by' ?? 
        remove_reason = subm['removed_by_category']
    except KeyError:
        remove_reason = ""
  except Exception:
    print("Error in record: ", subm)
    return

  data_dict['post_id'].append(id)
  data_dict['title'].append(title)
  data_dict['author'].append(auth)
  data_dict['text'].append(text)
  d = stampToDateObj(t)
  data_dict['stamp'].append(t)
  data_dict['date'].append(str(d.date()))
  data_dict['time'].append(str(d.time()))
  data_dict['score'].append(score)
  data_dict['comm'].append(com)
  data_dict['url'].append(link)
  data_dict['remove_reason'] = remove_reason


def startScraping(sub, after, before, query=None, size=500): # max:500
  '''
  Starting point of data scraping
  '''
  initDataDict(query)

  while True:
    data = getPostPushshiftData(sub, after, before, query, size)
    if len(data) == 0:
      print("empty data - no response")
      break
    for post in data:
      collectSubData(post)
    # update the starting date based on retrieved data
    after = data[-1]['created_utc']
    # wait 1 seconds, abiding Reddit rules (https://github.com/reddit-archive/reddit/wiki/API#rules)
    time.sleep(1)
    #break
  
  # fill repeated columns efficiently 
  fillMissingColumns('subreddit', sub)
  if query is not None:
    fillMissingColumns('query', query)

  # Show statistics
  statistics(sub, query)


def fillMissingColumns(col_name, col_content):
  '''
  To reduce processing time, you can use this method to fill in a column.
  This is not used .. may remove soon
  '''
  # Since multiple queries will be used for this project, the field is added
  # Other fields can be added this way (for now) like subreddit name and its ID
  data_dict[col_name] = [col_content] * len(data_dict['post_id'])


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
  try:
    df = pd.DataFrame(data_dict)
    df.to_csv(filename + ".csv")
    print("Data saved in " + filename + ".csv")
    df.to_excel(filename + ".xlsx")
    print("Data saved in " + filename + ".xlsx")

    ''' # this is the proper way to save
    if filename.endswith(".csv"):
      df.to_csv(filename)
    elif filename.endswith('xlsx'):
      df.to_excel(filename)
    else:
      # General case .. uses csv for now
      print("File extension is not supported, trying to save it as CSV")
      df.to_csv(filename)
    '''
  except Exception as e:
    print("Error saving data .. Message:\n", e)
  


if __name__ == '__main__':
  # Construct the argument parser
  ap = argparse.ArgumentParser()

  ap.add_argument("-r", "--subreddit", required=True, help="Subreddit Name")
  ap.add_argument("-q", "--query", required=False, help="Search term, if multiple please seperate by commas")
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
  #query = str(args.query).replace(',', '|')

  print("Arguments parsed .. Start scraping data \n\n")

  if args.query is not None:
    terms = str(args.query).split(',')
    for query in terms:
      print("Collecting data filtered by [{}]".format(query))
      startScraping(args.subreddit, query, after, before)
  else:
    print("Collecting data from [{}]".format(args.subbreddit))
    startScraping(args.subreddit, after, before)

  # Save results 
  saveResult(args.filename)

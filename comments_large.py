from asyncio.windows_events import NULL
import json
import argparse
import requests
import time
from util import *
from psaw import PushshiftAPI
from tqdm import tqdm


api = PushshiftAPI()

# Global dictionary to store the collected data
data_dict = {}
filtered_fields = ['parent_id', 'subreddit', 'id', 'author', 'body', 'score', 'permalink']

def initDataDict():
  '''
  Initilize the variable which stores the collected data
  * This is where you modify what data to be collected
  '''
  global fields_of_interest
  fields_of_interest = ['post_id', 'subreddit', 'comm_id', 'author', 'text', 'date', 'time', 'score', 'url', 'stamp']
  for f in fields_of_interest:
    data_dict[f] = []


def collectCommData(comment):
    '''
    Extract fields of interest from each comment, store results in 'data_dict'
    '''
    global data_dict

    data_dict['post_id'].append(comment.parent_id)
    data_dict['subreddit'].append(comment.subreddit)
    data_dict['comm_id'].append(comment.id)
    data_dict['author'].append(comment.author)
    data_dict['text'].append(comment.body)
    data_dict['score'].append(comment.score)
    data_dict['url'].append(comment.permalink)
    t = comment.created_utc
    d = stampToDateObj(t)
    data_dict['stamp'].append(t)
    data_dict['date'].append(str(d.date()))
    data_dict['time'].append(str(d.time()))


def startScraping(sub, after, before, query=None, size=1000):
    '''
    The starting point of collecting commnets from 'posts' of interest
    Posts ids are extracted from input file.
    '''
    initDataDict()
    end_epoch = before
    while True:
        data_gen = api.search_comments(subreddit=sub, after=after, before=end_epoch, filter=filtered_fields, q=query, limit=1000)
        #data_gen = api.search_comments(subreddit=sub, before=before, filter=filtered_fields, q=query, limit=2000)
        for comm in tqdm(data_gen, desc="Processing patch progress", ncols=50):
            collectCommData(comm)
        # update the starting date based on retrieved data
        if len(data_dict['stamp']) == 0:
          print("No data available that satisfies the given parameters\n")
          break
        elif end_epoch == data_dict['stamp'][-1] or after > end_epoch:
          print("finished scraping comments")
          break
        end_epoch = data_dict['stamp'][-1]
        print("Records collected so far: ", len(data_dict["post_id"]))
        print("Start Date: {}, End Date: {}, Current Date: {}\n".format(stampToDateObj(after).date(), stampToDateObj(before).date(), stampToDateObj(end_epoch).date()))
        #time.sleep(1) # processing collected data takes time, no need to wait

def statistics():
  '''
  Prints basic statistics of collected data
  '''
  print("*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-")
  print("Session data statistics: \n")
  print("Comments scraped: {}".format(len(data_dict['post_id'])))
  print("From: {}".format(str(stampToDateObj(data_dict['stamp'][0]))))
  print("Until: {}".format(str(stampToDateObj(data_dict['stamp'][-1]))))
  print("Subreddit: {}".format(data_dict['subreddit'][0]))
  print("*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-")


if __name__ == '__main__':
  # Construct the argument parser
  ap = argparse.ArgumentParser()

  ap.add_argument("-r", "--subreddit", required=True, help="Subreddit Name")
  ap.add_argument("-q", "--query", required=False, help="Search term, if multiple please seperate by commas")
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

  if args.query is not None:
    terms = str(args.query).split(',')
    for query in terms:
      print("Collecting data filtered by [{}]".format(query))
      startScraping(args.subreddit, after, before, query)
  else:
    print("Collecting data from [{}]".format(args.subreddit))
    startScraping(args.subreddit, after, before)

  if len(data_dict['post_id']) == 0:
    print("No data was collected. Try changing filtering parameters.")
  else:
    # Convert data from dictionary form to dataframe
    df = pd.DataFrame(data_dict)
    # remove duplicates based on ID
    #df = df.drop_duplicates(['comm_id'])
    # Save results 
    saveResult(args.filename, df)

    # show stats of collected data
    statistics()

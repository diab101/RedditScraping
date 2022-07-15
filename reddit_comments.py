import json
import argparse
import requests
import time
from util import *



# Global dictionary to store the collected data
data_dict = {}


def initDataDict():
  '''
  Initilize the variable which stores the collected data
  * This is where you modify what data to be collected
  '''
  fields_of_interest = ['post_id', 'subreddit', 'comm_id', 'author', 'text', 'date', 'time', 'score', 'url', 'stamp']
  for f in fields_of_interest:
    data_dict[f] = []


def requestDataFromUrl(url):
    '''
    Request data based on url, dealing with possible exceptions
    '''
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


def buildCommentIDsUrl(post_id):
    '''
    Builds PushShift url to retrieve comments ids of a single post
    '''
    url = "https://api.pushshift.io/reddit/submission/comment_ids/" + str(post_id)
    return url


def buildCommentsUrl(comm_ids):
    ids = ','.join(comm_ids)
    url = "https://api.pushshift.io/reddit/comment/search?ids=" + ids
    return url

def collectCommData(comment, pid):
    '''
    Extract fields of interest from each comment, store results in 'data_dict'
    '''
    global data_dict

    # clear previous data, just in case
    subr, id, auth, text, t, score, link = " " * 7

    try: # to avoid script crashing 
        subr = comment['subreddit']
        id = comment['id']
        auth = comment['author']
    #try:
        text = comment['body']
    #except KeyError: # *** not sure how banned comments look like
    #  text = "[banned]"
        t = comment['created_utc']
        score = comment['score']
        link = comment['permalink']    
    except Exception:
        print("Error in record: ", comment)
        return

    data_dict['post_id'].append(pid)
    data_dict['subreddit'].append(subr)
    data_dict['comm_id'].append(id)
    data_dict['author'].append(auth)
    data_dict['text'].append(text)
    d = stampToDateObj(t)
    data_dict['stamp'].append(t)
    data_dict['date'].append(str(d.date()))
    data_dict['time'].append(str(d.time()))
    data_dict['score'].append(score)
    data_dict['url'].append(link)


def startScraping(inputfile):
    '''
    The starting point of collecting commnets from 'posts' of interest
    Posts ids are extracted from input file.
    '''
    initDataDict()

    # Obtain a list of unique post IDs
    posts_ids = retrievePostIds(inputfile)

    for pid in posts_ids:
        url1 = buildCommentIDsUrl(pid)
        comm_ids = requestDataFromUrl(url1)
        url2 = buildCommentsUrl(comm_ids)
        comm_list = requestDataFromUrl(url2)
        for comment in comm_list:
            collectCommData(comment, pid)



if __name__ == '__main__':
  # Construct the argument parser
  ap = argparse.ArgumentParser()

  ap.add_argument("-i", "--input", required=True, help="Input file to read post IDs")
  ap.add_argument("-o", "--output", required=False, help="Output file to save the collected comments")

  args = ap.parse_args()

  print("Arguments parsed .. Start scraping data \n\n")
  startScraping(args.input)

  # Save results 
  saveResult(args.output, data_dict)

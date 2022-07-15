import json
import pandas as pd
from collections import Counter
from datetime import datetime


def stampToDateObj(t):
  return datetime.fromtimestamp(t)


def dateToStamp(d):
  return datetime.timestamp(d)


def usersStats(filename, col_name='author'):
  '''
  Find user statistics (Total, unique, histogram)
  '''
  df = pd.read_csv(filename, usecols = [col_name])
  users = df[col_name].values.tolist()
  unique_users = set(users)
  print("Total users: ", len(users))
  print("Unique users: ", len(unique_users))
  histo_data = Counter(users)
  print("Histogram of users activity: \n", histo_data)


def queryStats(filename, col_name='query'):
    '''
    Prints query histogram - how many items retrieved via a specific query
    '''
    df = pd.read_csv(filename, usecols = [col_name])
    queries = df[col_name].values.tolist()
    unique_queries = set(queries)
    print("Total users: ", len(queries))
    print("Unique users: ", len(unique_queries))
    histo_data = Counter(queries)
    print("Histogram of query success: \n", histo_data)


def dateStats(filename, col_name='date'):
  '''
  Aggregates data by day and month, prints dictionary results
  '''
  df = pd.read_csv(filename, usecols = [col_name])
  dates = df[col_name].values.tolist()
  histo_day = Counter(dates)
  months = [d[:-3] for d in dates]
  histo_month = Counter(months)
  print("Aggregated by day: ", histo_day)
  print("Aggregated by month: ", histo_month)


def retrievePostIds(filename, save=False, savefile='post_ids.json', col_name='post_id'):
  '''
  Prints post ids, useful for comment scraping. 
  If 'save' flag is true, the results are stored in a json file
  '''
  df = pd.read_csv(filename, usecols = [col_name])
  p_id = df[col_name].values.tolist()
  up_id = set(p_id)
  print("Total posts: ", len(up_id))
  print("IDs: \n", up_id)
  if save:
    print("saving results to ", savefile)
    with open(savefile, "w") as outfile:
      json.dump(up_id, outfile)
  
  return up_id


def saveResult(filename, data_dict):
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
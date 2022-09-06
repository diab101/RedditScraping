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


def saveResult(filename, df):
  '''
  Saves the collected data to a csv file
  '''

  print("Saving results in csv and excel format, file name: ", filename)
  try:
    df.to_csv(filename + ".csv")
    print("Data saved in " + filename + ".csv")
  except Exception as e:
    print("Error saving in csv format .. Message:\n", e)

  try:
    df.to_excel(filename + ".xlsx")
    print("Data saved in " + filename + ".xlsx")
  except Exception as e:
    print("Error saving excel format .. Message:\n", e)


def mergeCSV(f1, f2, remove_duplicate=False, col_name='post_id'):
  '''
  Merges two csv files, saves the output 
  '''
  df1 = pd.read_csv(f1)
  df2 = pd.read_csv(f2)
  df = pd.concat([df1, df2])
  if remove_duplicate:
    df = df.drop_duplicates([col_name])
  df.to_csv("merged.csv")
import pandas as pd
from collections import Counter


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


def retrievePostIds(filename, col_name='post_id'):
  '''
  Prints post ids, useful for comment scraping
  '''
  df = pd.read_csv(filename, usecols = [col_name])
  p_id = df[col_name].values.tolist()
  up_id = set(p_id)
  print("Total posts: ", len(up_id))
  print("Content: \n", up_id)
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 27 11:40:38 2019

@author: JAB
"""

#!/usr/bin/python3
"""
TODOs:
    •	Use ‘OR’ operation to show only the ‘summary’ and ‘overall’ columns with overall rating 4 or 1.
    •	Use ‘AND’ operation to show only the ‘summary’ and ‘overall’ columns with overall rating between 1 and 4
    •	Group by ‘Revewer Name’ to show the minimum ‘overall’ rating they posted, sorted by descending alphabetic order of the ‘reviewer name’.
    •	Group by ‘Helpful’ to show the ‘total number’ of entries found for different ‘Helpful’ data, sorted by descending order of the ‘total number’.
    •	Show ‘Review Text’ that contains at least one numeric value.

"""

#%%
from pymongo import MongoClient
import pandas as pd
from bson import SON
#%%PARSE DATA
path = r"YOURPATH/Data/Movies_and_TV_5.json"
import json
data_file = []
with open(path, 'r') as infile:
    for line in infile:
        data_file.append(json.loads(line))
#%%ESTBALISH CONNECTION WITH MongoDB
client = MongoClient("mongodb://localhost:27017/")
print("Connection Successful")
#%%TEST SAMPLE FOR QUICK ITERATIONS/RUNS
test_data_file = data_file[0:5000]
#%%INSERT DATA
"""
Note - dataframes created to capture outputs for ease of further manipulation within python environment. Not really required
       but since a test sample is used, data can be explored further for other purposes e.g if sizeable batches are needed for 
       reporting or analysis...
"""
with client:
    testdb = client["movies"]
    dblist = client.list_database_names()
    if "movies" in dblist:
        print("The database exists.")
    #Create collection name called mymovies
    mycol = testdb["mymovies"]
    collist = testdb.list_collection_names()
    if "mymovies" in collist:
        print("The collection exists.")
    #Insert documents into mymovies collection
    data = mycol.insert_many(test_data_file)
#%%QUERY FROM TABLE
neat, neat2, neat3, neat4, neat5 = [], [], [], [], []
with client:
    #print(mycol.find_one())
    """
    Use ‘OR’ operation to show only the ‘summary’ and ‘overall’ columns with overall rating 4 or 1.
    """
    #myquery = {"$or":[{"overall":{"$gt":4}},{"overall":{"$gt":1}}]}
    myquery1 = {"$or":[{"overall":4},{"overall":1}]}
    result1 = mycol.find(myquery1)
    
    for result in result1:
        print(result)
        neat.append(result)
        df = pd.DataFrame(neat)
        df = df[["summary","overall"]]
    """
    Use ‘AND’ operation to show only the ‘summary’ and ‘overall’ columns with overall rating between 1 and 4
    """
    myquery2 = {"$and":[{"overall":{"$lte":4}},{"overall":{"$gte":1}}]}
    result2 = mycol.find(myquery2)
    
    for result in result2:
        print(result)
        neat2.append(result)
        df2 = pd.DataFrame(neat2)
        df2 = df2[["summary","overall"]]
    """
    Group by ‘Revewer Name’ to show the minimum ‘overall’ rating they posted, 
    sorted by descending alphabetic order of the ‘reviewer name’.
    """
# =============================================================================
#     pipeline = [{"$group": {"_id": "$reviewerName", "count": {"$sum": 1}}},
#                 {"$sort": SON([("reviewerName", -1)])}]
#     pipeline = [{"$group": {"_id": "$reviewerName", "grpmin": {"$min": "$overall"}}},
#                 {"$sort": SON([("reviewerName", -1)])}]
# =============================================================================
    pipeline = [{"$group": {"_id": "$reviewerName", "grpmin": {"$min": "$overall"}}},
                {"$sort": {"reviewerName":-1}}]
    result3 = mycol.aggregate(pipeline)
   
    
    for result in result3:
        print(result)
        neat3.append(result)
        df3 = pd.DataFrame(neat3)
        
    """
    Group by ‘Helpful’ to show the ‘total number’ of entries found for different ‘Helpful’ data, 
    sorted by descending order of the ‘total number’.
    """
    pipeline2 = [{"$group": {"_id": "$helpful", "total": {"$sum": 1}}},
                {"$sort": SON([("total", -1)])}]
    result4 = mycol.aggregate(pipeline2)
    
    for result in result4:
        print(result)
        neat4.append(result)
        df4 = pd.DataFrame(neat4)
        
    """
    Show ‘Review Text’ that contains at least one numeric value.
    """
    result5 = mycol.find(
            {"reviewText":{"$regex":"/[0-9]+/"}}, no_cursor_timeout = True
            )
    
    for result in result5:
        print(result)
        neat5.append(result)
        df5 = pd.DataFrame(neat5)["reviewText"]
#%%
                

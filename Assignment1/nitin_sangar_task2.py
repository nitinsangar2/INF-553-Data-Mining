from pyspark.sql import SparkSession
from pyspark import SparkContext
import os

import json
import operator
import sys
import time

progStart = time.time()


inputPath1 = sys.argv[1]
inputPath2 = sys.argv[2]
output1 = sys.argv[3]
output2 = sys.argv[4]



sc = SparkContext('local[*]','nitin_sangar_scc')

#Reading reviews data
reviewData = sc.textFile(inputPath1).map(json.loads)

#Reading business data
businessData = sc.textFile(inputPath2).map(json.loads)

#Get businessID, Stars
businessStars = reviewData.map(lambda x: (x['business_id'],x['stars']))

#Get businessID, State
businessState = businessData.map(lambda x: (x['business_id'], x['state']))

#Join the two databases
starsState = businessStars.join(businessState)

#Get stars by state
starsByState = starsState.map(lambda x: (x[1][1], x[1][0]))


#Count number of stars by state
NumOfStarsByState = starsByState.countByKey()

#Count sum of stars by state
sumOfStarsByState = starsByState.reduceByKey(lambda x,y:x+y)

#Count average
averageByState = sumOfStarsByState.map(lambda x:(x[0],(x[1]/NumOfStarsByState[x[0]])))
sortedAverageByState = averageByState.sortBy(lambda x:(-x[1], x[0]))

listAverageByState = sortedAverageByState.collect()
file = open(output1, 'w')
file.write("state,stars\n")
for state,avg in listAverageByState:
    file.write(state)
    file.write(",")
    file.write(str(avg))
    file.write("\n")

# Check the collect method
start = time.time()
listAvg = sortedAverageByState.collect()
for i in range(0,5):
    print("{}\n".format(listAvg[i][0]))
timeFirst = time.time() - start

#checking the take method
start2 = time.time()
listAvg1 = sortedAverageByState.take(5)
for state,avg in listAvg1:
    print("{}\n".format(state))
timeSecond = time.time() - start2
result = {}
result['m1'] = timeFirst
result['m2'] = timeSecond
result['explanation'] = "Collect() collects all the element first and store them in the list and then we are printing the top 5 whereas in take() we are just taking the first five elements and not iterating over all the elements"
with open(output2, 'w') as outfile:
    json.dump(result, outfile, indent=2)

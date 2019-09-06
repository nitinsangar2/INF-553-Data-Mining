from pyspark.sql import SparkSession
from pyspark import SparkContext
import os

import json
import operator
import sys


inputPath = sys.argv[1]
outputPath = sys.argv[2]


sc = SparkContext('local[*]','nitin_sangar_sc')
# Task 1
data = sc.textFile(inputPath).map(json.loads).map(lambda x:(x['useful'], x['stars'], x['text'], x['user_id'], x['business_id']))

result = {}
#Useful reviews
usefulReviews = data.filter(lambda x: x[0] > 0)
#print('\nNumber of Useful Reviews : {}'.format(usefulReviews.count()))

result["n_review_useful"] = usefulReviews.count()

#Number of reviews that have five star rating
fiveRatingReviews = data.filter(lambda x: x[1] == 5).count()
#print('\nNumber of reviews with 5 star : {}'.format(fiveRatingReviews.count()))
result["n_review_5_star"] = fiveRatingReviews

#characters in the text of longest review
maxReview = data.max(lambda x:len(x[2]))
#print("\nCharacters in Max Review : {}".format(len(maxReview['text'])))
result['n_characters'] = len(maxReview[2])

#Number of distinct users who wrote reviews
distinctCount = data.map(lambda x:x[3]).distinct().count()
#print("\nNumber of Distinct Users : {}".format(distinctCount))
result['n_user'] = distinctCount

#Top 20 users who wrote the largest number of Reviews
users = data.map(lambda x:(x[3],1))
userRDD = users.countByKey()
sorted_Users = sorted(userRDD.items(), key=lambda e:(-e[1],e[0]))
i = 0;
userData = []
#print('\n Top 20 Users - ')
for user,reviews in sorted_Users:
    userData.append([user, reviews])
    i += 1
    if i == 20:
        break;
result['top20_user'] = userData

#Number of distinct business that have been reviewed
distinctBusiness = data.map(lambda x: x[4]).distinct().count()
result['n_business'] = distinctBusiness

#Top 20 business
business = data.map(lambda x: (x[4],1))
businessRDD = business.countByKey()
sorted_business = sorted(businessRDD.items(), key=lambda e:(-e[1],e[0]))
i = 0;
businessData = []
#print('\n Top 20 Business - ')
for business,reviews in sorted_business:
    businessData.append([business, reviews])
    i += 1
    if i == 20:
        break;
result['top20_business'] = businessData
with open(outputPath, 'w') as outfile:
    json.dump(result, outfile, indent=2)
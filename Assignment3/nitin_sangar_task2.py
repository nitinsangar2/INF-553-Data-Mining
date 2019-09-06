from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

import time
import sys

start = time.time()
sc = SparkContext('local[*]','nitin_sangar_sc')

train_file_path = sys.argv[1]
test_file_path = sys.argv[2]
case = int(sys.argv[3])
outPath = sys.argv[4]
outfile = open(outPath, 'w')

# Making dictionaries
data = sc.textFile(train_file_path)
validationData = sc.textFile(test_file_path)
firstRow = data.first()
first = validationData.first()


# For user and item based
businessRatingsPerUser = data.filter(lambda row: row != firstRow).map(lambda x: x.split(",")).map(
    lambda x: (x[0], (x[1], float(x[2])))).groupByKey().mapValues(lambda x: list(set(list(x))))

userBusiness = {}
businessUser = {}
for e in businessRatingsPerUser.collect():
    userBusiness[e[0]] = e[1]
usersPerBusiness = data.filter(lambda row: row != firstRow).map(lambda x: x.split(",")).map(
    lambda x: (x[1], (x[0], float(x[2])))).groupByKey().mapValues(lambda x: list(set(list(x))))

for e in usersPerBusiness.collect():
    businessUser[e[0]] = e[1]

def model_based_cf():
    firstRow = data.first()
    first = validationData.first()
    user1 = data.filter(lambda row: row != firstRow).map(lambda x: x.split(",")).map(
        lambda x: x[0]).distinct().collect()
    user2 = validationData.filter(lambda row: row != first).map(lambda x: x.split(",")).map(
        lambda x: x[0]).distinct().collect()
    bus1 = data.filter(lambda row: row != firstRow).map(lambda x: x.split(",")).map(lambda x: x[1]).distinct().collect()
    bus2 = validationData.filter(lambda row: row != first).map(lambda x: x.split(",")).map(
        lambda x: x[1]).distinct().collect()
    users = list(set(user1) | set(user2))
    business = list(set(bus1) | set(bus2))

    userDict = {}
    busDict = {}
    revUserDict = {}
    revBusDict = {}
    for i in range(0, len(users)):
        userDict[users[i]] = i
        revUserDict[i] = users[i]

    for i in range(0, len(business)):
        busDict[business[i]] = i
        revBusDict[i] = business[i]

    firstRow = data.first()
    trainData = data.filter(lambda row: row != firstRow).map(lambda x: x.split(",")).map(
        lambda x: (int(userDict[x[0]]), int(busDict[x[1]]), float(x[2])))

    rank = 2
    iterations = 20
    model = ALS.train(trainData, rank, iterations, lambda_=0.5)

    validationTest = validationData.filter(lambda row: row != first).map(lambda x: x.split(",")).map(
        lambda x: (int(userDict[x[0]]), int(busDict[x[1]])))
    testList = validationTest.collect()
    predictions = model.predictAll(validationTest).map(lambda x: ((x[0], x[1]), x[2]))
    ratings = predictions.map(lambda x: x[1]).collect()
    avgRating = sum(ratings)/len(ratings)
#    print("avg rating is {}".format(avgRating))
    ratingsList = predictions.collect()
    ratingsDict = {k:v for k,v in ratingsList}
    outfile.write("user_id, business_id, prediction")
    for rating in testList:
        outfile.write("\n")
        if (rating[0],rating[1]) in ratingsDict:
            outfile.write(str(revUserDict[rating[0]])+","+str(revBusDict[rating[1]])+","+str(ratingsDict[(rating[0],rating[1])]))
        else:
            outfile.write(str(revUserDict[rating[0]]) + "," + str(revBusDict[rating[1]]) + "," +str(avgRating))

#    print("Duration :{}".format(time.time() - start))

def predict_user_based(x):
    user = x[0]
    business = x[1]
    if user not in userBusiness:
        return -1.0

    bRDict1 = {k: v for k, v in userBusiness[user]}
    avgR = float(sum(bRDict1.values()))/float(len(bRDict1))
    if business in businessUser:
        userList = businessUser[business]
    else:
        return avgR
    numerator = 0.0
    denominator = 0.0
    for u, r in userList:
        bRDict2 = {k:v for k, v in userBusiness[u]}
        commonList = [(bRDict1[k], bRDict2[k]) for k in (bRDict1.keys() & bRDict2.keys())]
        length = len(commonList)
        if length == 0:
            continue
        sum1 = 0.0
        sum2 = 0.0
        for k in commonList:
            sum1 += float(k[0])
            sum2 += float(k[1])

        avg1 = float(sum1) / float(length)
        avg2 = float(sum2) / float(length)

        num = 0.0
        deno1 = 0.0
        deno2 = 0.0
        for k in commonList:
            num += float((k[0] - avg1) * (k[1] - avg2))
            deno1 += float(k[0] - avg1) ** 2
            deno2 += float(k[1] - avg2) ** 2

        deno1 = pow(deno1, 0.5)
        deno2 = pow(deno2, 0.5)
        deno = float(deno1 * deno2)
        if deno != 0.0:
            sim = float(float(num)/float(deno))
        else:
            sim = 0.1

        numerator += float((r - avg2) * sim)
        denominator += float(abs(sim))

    if denominator == 0.0:
        return avgR
    rating = float(avgR + float(numerator) / float(denominator))
    if rating > 5.0:
        return 5.0
    if rating < 0.0:
        return 0.0
    return rating

def user_based_cf():
    firstRow = data.first()
    first = validationData.first()
    user1 = data.filter(lambda row: row != firstRow).map(lambda x: x.split(",")).map(
        lambda x: x[0]).distinct().collect()
    user2 = validationData.filter(lambda row: row != first).map(lambda x: x.split(",")).map(
        lambda x: x[0]).distinct().collect()
    diffusers = list(set(user2) - set(user1))
    firstRow = validationData.first()
    testData = validationData.filter(lambda x: x != firstRow).map(lambda x: x.split(",")).map(
        lambda x: ((x[0], x[1]), float(x[2])))
    ratings = testData.map(lambda x: (x[0], (predict_user_based(x[0]), x[1])))

    ratingsList = ratings.collect()
    su = 0.0
    count = 0
    for rating in ratingsList:
        if rating[1][0] == -1.0:
            continue
        else:
            su += rating[1][0]
            count += 1
    avgRating = float(su) / float(count)
#   print("avg rating is {}".format(avgRating))

    outfile.write("user_id, business_id, prediction")
    for r in ratingsList:
        outfile.write("\n")
        if r[0][0] in diffusers:
            outfile.write(str(r[0][0]) + "," + str(r[0][1]) + "," + str(avgRating))
        else:
            outfile.write(str(r[0][0]) + "," + str(r[0][1]) + "," + str(r[1][0]))
#   print("Duration :{}".format(time.time()-start))


def predict_item_based(x):
    user = x[0]
    business = x[1]
    if business not in businessUser:
        return -1.0

    uRDict1 = {k: v for k, v in businessUser[business]}
    avgR = float(float(sum(uRDict1.values())) / float(len(uRDict1)*1.0))
    if user in userBusiness:
        busList = userBusiness[user]
    else:
        return avgR
    numerator = 0.0
    denominator = 0.0
    for b,r in busList:
        uRDict2 = {k:v for k, v in businessUser[b]}
        commonList = [(uRDict1[k], uRDict2[k]) for k in (uRDict1.keys() & uRDict2.keys())]
        length = len(commonList)
        if length == 0:
            continue
        sum1 = 0.0
        sum2 = 0.0
        for k in commonList:
            sum1 += float(k[0])
            sum2 += float(k[1])

        avg1 = float(sum1)/float(length)
        avg2 = float(sum2)/float(length)

        num = 0.0
        deno1 = 0.0
        deno2 = 0.0
        for k in commonList:
            num += float((k[0] - avg1)*(k[1] - avg2))
            deno1 += float(k[0] - avg1)**2
            deno2 += float(k[1] - avg2)**2

        deno1 = pow(deno1, 0.5)
        deno2 = pow(deno2, 0.5)
        deno = float(deno1*deno2)
        if deno != 0.0:
            sim = float(float(num)/float(deno))
        else:
            sim = 0.1

        numerator += float((r - avg2) * sim)
        denominator += float(abs(sim))

    if denominator == 0.0:
        return avgR
    rating = avgR + float(numerator) / float(denominator)
    if rating > 5.0:
        return 5.0
    if rating < 0.0:
        return 0.0
    return rating


def item_based_cf():
    firstRow = data.first()
    first = validationData.first()
    bus1 = data.filter(lambda row: row != firstRow).map(lambda x: x.split(",")).map(lambda x: x[1]).distinct().collect()
    bus2 = validationData.filter(lambda row: row != first).map(lambda x: x.split(",")).map(
        lambda x: x[1]).distinct().collect()
    diffBusiness = list(set(bus2) - set(bus1))

    firstRow = validationData.first()
    testData = validationData.filter(lambda x: x != firstRow).map(lambda x: x.split(",")).map(lambda x: ((x[0], x[1]), float(x[2])))
    ratings = testData.map(lambda x: (x[0], (predict_item_based(x[0]), x[1])))
    ''
    ratingsList = ratings.collect()
#    print("Mapping done in {}".format(time.time()-start))
    su = 0.0
    count = 0
    for rating in ratingsList:
        if rating[1][0] == -1.0:
            continue
        else:
            su += rating[1][0]
            count += 1
    avgRating = float(su) / float(count)
 #   print("avg rating is {}".format(avgRating))

    outfile.write("user_id, business_id, prediction")
    for r in ratingsList:
        outfile.write("\n")
        if r[0][1] in diffBusiness:
            outfile.write(str(r[0][0]) + "," + str(r[0][1]) + "," + str(avgRating))
        else:
            outfile.write(str(r[0][0]) + "," + str(r[0][1]) + "," + str(r[1][0]))
    print("Duration: {}".format(time.time() - start))

def task(case):
    if case == 1:
        model_based_cf()
    elif case == 2:
        user_based_cf()
    else:
        item_based_cf()

if __name__ == '__main__':
    task(case)
from pyspark import SparkContext
import itertools
import sys
import time
start = time.time()
sc = SparkContext('local[*]','nitin_sangar_sc')


# Creating user and business baskets
case = int(sys.argv[1])
support = int(sys.argv[2])
inputFile = sys.argv[3]
outPath = sys.argv[4]
outfile = open(outPath, 'w')

#Reading Data
data = sc.textFile(inputFile)

if case == 1:
    firstRow = data.first()
    baskets = data.filter(lambda row: row != firstRow).map(lambda x: x.split(",")).groupByKey().mapValues(
        lambda x: list(set(x)))
else:
    firstRow = data.first()
    baskets = data.filter(lambda row: row != firstRow).map(lambda x: x.split(",")).map(
        lambda x: (x[1], x[0])).groupByKey().mapValues(lambda x: list(set(x)))


def apriori(iterator, support, totalBaskets):
    basketList = list(iterator)
    chunkBaskets = len(basketList)
    s = ((support)*(float(chunkBaskets)/float(totalBaskets)))
    cItemSetList = []
    countItemSet = {}
    for basket in basketList:
        for item in basket:
            if item in countItemSet:
                countItemSet[item] += 1
            else:
                countItemSet[item] = 1
    for k, v in countItemSet.items():
        if v >= s:
            cItemSetList.append(k)

    cItemSetList = sorted(cItemSetList)
    # print("len of cItemSetList is {}".format(len(cItemSetList)))
    # print("s is {}".format(s))
    candidateList = []
    candidateList.extend(cItemSetList)
    # print('here')
    i = 2
    # print("s is {}".format(s))
    while (len(cItemSetList) > 0):
        cItemSetList = getCandidates(cItemSetList, i)
        #   print("len of candidatelist is {}".format(len(cItemSetList)))
        countItemSet = {}
        resultSet = []

        for itemSet in cItemSetList:
            items = set(itemSet)
            #  print("items is {}".format(items))
            for basket in basketList:
                #  print("items is {}".format(items))
                if items.issubset(basket):
                    #   print("in here")
                    if itemSet in countItemSet:
                        countItemSet[itemSet] += 1
                    else:
                        countItemSet[itemSet] = 1
        for k, v in countItemSet.items():
            if v >= s:
                resultSet.append(k)
        cItemSetList = resultSet
        #  print("length that appends to candidate is {}".format(len(cItemSetList)))
        if len(cItemSetList) == 0:
            break
        candidateList.extend(cItemSetList)
        i += 1

    return candidateList

def apriori1(iterator, candidateSet):
    itemCount = {}
    basketList = list(iterator)
    for candidate in candidateSet:
        for basket in basketList:
            if type(candidate) == str:
               # print("here")
                items = set()
                items.add(candidate)
                tup = candidate
            else:
                items = set(candidate)
                tup = candidate
            if items.issubset(basket):
                if tup not in itemCount:
                    itemCount[tup] = 1;
                else:
                    itemCount[tup] += 1
    return itemCount.items()

def getCandidates(candidateList, index):
    resultSet = []
    resultList = []
    length = len(candidateList)
    # print('size of cItemSetList is {}'.format(len(cItemSetList)))
    if index == 2:
        for x in range(0, length - 1):
            for y in range(x + 1, length):
                a = candidateList[x]
                b = candidateList[y]
                c = list(set([a,b]))
                c.sort()
                resultSet.append(tuple(c))
        return resultSet
    else:
        for x in range(0, length - 1):
            for y in range(x + 1, length):
                a = list(candidateList[x])
                b = list(candidateList[y])
                if a[0:index-2] == b[0:index-2]:
                    c = list(set(a) | set(b))
                    c.sort()
                    resultSet.append(tuple(c))

    for item in resultSet:
        combinations = itertools.combinations(item, index + 1)
        flag = False
        for comb in combinations:
            if comb not in candidateList:
                flag = True
                break
        if flag == True:
            continue
        resultList.append(item)
    return resultList


def writeToFile(cItemSetList, index):
    if index == 0:
        l = len(cItemSetList)
        # print("Size of candidate list is {}".format(len(cItemSetList)))
        for i in range(0, l):
            item = cItemSetList[i]
            if i == l - 1:
                outfile.write("('" + item + "')")
            else:
                outfile.write("('" + item + "'),")
    else:
        l = len(cItemSetList)
        # print("Size of candidate list is {}".format(len(cItemSetList)))
        for i in range(0, l):
            item = cItemSetList[i]
            if i == l - 1:
                outfile.write(str(item))
            else:
                outfile.write(str(item) + ",")

def task(support):
    basketValues = baskets.values()
    totalBaskets = basketValues.count()
    candidateList =  basketValues.mapPartitions(lambda x: apriori(x, support, totalBaskets)).distinct()
    singleCandidates = candidateList.filter(lambda x: type(x) == str).collect()
    singleCandidates.sort()
    restCandidateItems = candidateList.filter(lambda x: type(x) != str)
    restList = restCandidateItems.collect()

    l = len(max(restList, key=len))

    outfile.write("Candidates:\n")
    length = len(singleCandidates)
    for i in range(0, length):
        item = singleCandidates[i]
        if i == length - 1:
            outfile.write("('" + item + "')")
        else:
            outfile.write("('" + item + "'),")

    for i in range(2, l + 2):
        itemList = restCandidateItems.filter(lambda x: (len(x) == i)).collect()
        if len(itemList) == 0:
            break
        itemList.sort()
        l = len(itemList)
        # print(l)
        outfile.write("\n\n")
        for i in range(0, l):
            item = itemList[i]
            if i == l - 1:
                outfile.write(str(item))
            else:
                outfile.write(str(item) + ",")

    #Phase 2
    candidateList = candidateList.collect()

    frequentItemsets = basketValues.mapPartitions(lambda x: apriori1(x, candidateList)).reduceByKey(lambda x,y:x+y).filter(lambda x: x[1]>=support).keys()
    singleFrequentItemsets = frequentItemsets.filter(lambda x: type(x) == str).collect()
    singleFrequentItemsets.sort();
    #print(singleFrequentItemsets)
    restFrequentItemsets = frequentItemsets.filter(lambda x:type(x) != str)
    restList = restFrequentItemsets.collect()

    l = len(max(restList, key=len))
    #  print("l is {}".format(l))
    # map(lambda x:x[0]).sortByKey().collect()

   # print("frequent start")
    outfile.write("\n\n")
    outfile.write("Frequent Itemsets:\n")
    length = len(singleFrequentItemsets)
  #  print(length)
    for i in range(0, length):
        item = singleFrequentItemsets[i]
        if i == length - 1:
            outfile.write("('" + item + "')")
        else:
            outfile.write("('" + item + "'),")

    for i in range(2, l + 2):
        itemList = restFrequentItemsets.filter(lambda x: (len(x) == i)).collect()
        if len(itemList) == 0:
            break
        itemList.sort()
        l = len(itemList)
       # print(l)
        outfile.write("\n\n")
        for i in range(0, l):
            item = itemList[i]
            if i == l - 1:
                outfile.write(str(item))
            else:
                outfile.write(str(item) + ",")

if __name__ == '__main__':
    task(support)
    print("Duration: {}".format((time.time()-start)))
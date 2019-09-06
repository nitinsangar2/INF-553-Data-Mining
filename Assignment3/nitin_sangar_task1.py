from pyspark import SparkContext
import math
import time
import sys
start = time.time()
sc = SparkContext('local[*]','nitin_sangar_sc')


inputPath = sys.argv[1]
outPath = sys.argv[2]
outfile = open(outPath, 'w')

data = sc.textFile(inputPath)

firstRow = data.first()
businessPerUser = data.filter(lambda row: row != firstRow) 
users = businessPerUser.keys().distinct().collect()
userPerBusiness = data.filter(lambda row: row != firstRow).map(lambda x: x.split(",")).map(lambda x: (x[1], x[0])).groupByKey().mapValues(lambda x: list(set(list(x))))
uBList = userPerBusiness.collect()
business = userPerBusiness.keys().collect()
numOfBusiness = len(business)
numOfUsers = len(users)
# print("num of business is {}".format(numOfBusiness))
# print("num of users is {}".format(len(users)))
indexDict = {}
busDict = {}
for i in range(0, len(users)):
    indexDict[users[i]] = i

for i in range(0, len(business)):
    busDict[business[i]] = i

n = 200
b = 100
r = 2

arr = []
brr = []
mrr = []

def isPrime(n):
    # Corner cases
    if (n <= 1):
        return False
    if (n <= 3):
        return True

    # This is checked so that we can skip
    # middle five numbers in below loop
    if (n % 2 == 0 or n % 3 == 0):
        return False

    i = 5
    while (i * i <= n):
        if (n % i == 0 or n % (i + 2) == 0):
            return False
        i = i + 6

    return True

primes = [i for i in range(1,1000000) if isPrime(i)]

for i in range(0,n):
    arr.append(11*(i+1))
    brr.append(23*(i+1))
    mrr.append(primes[len(primes)-1-i])

def func(iterator, a, b, m):
    mini = math.inf
    usersList = list(iterator)
    for user in usersList:
        i = indexDict[user]
        val = ((a * i) + b) % m
        mini = min(val, mini)
    return mini


def func1(iterator):
    hashList = []
    for i in range(0, n):
        a = arr[i]
        b = brr[i]
        m = mrr[i]
        hashList.append(func(iterator,a,b,m))
    return hashList

def func2(iterator):
    sig = list(iterator)
  #  print("size is {}".format(len(sig[1])))
    hashList = []
    for i in range(0,b):
        myList = [i]
        for j in range((i*r),(i+1)*r):
            myList.append(sig[1][j])
        hashList.append((tuple(myList), [sig[0]]))
    return hashList

def getCandidates(iterator):
    l = list(iterator)
    l.sort()
    length = len(l)
    candidates = []
    for i in range(0, length):
        for j in range(i+1, length):
            candidates.append(((l[i], l[j]),1))
    return candidates

def jacc(iterator):
    bus1 = iterator[0]
    bus2 = iterator[1]
    bus1set = set(uBList[busDict[bus1]][1])
    bus2set = set(uBList[busDict[bus2]][1])
    u = bus1set | bus2set
    inter = bus1set & bus2set
    sim = len(inter)/len(u)
    return (bus1, bus2,sim)

# Calculate signature matrix
signature = userPerBusiness.mapValues(lambda x: func1(x))

signatureByBands = signature.flatMap(func2).reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1])>1).map(lambda x: x[1])

candidates = signatureByBands.flatMap(getCandidates).keys().distinct()

candidatesAfterJaccard = candidates.map(jacc).filter(lambda x: x[2]>=0.5).distinct().collect()
candidatesAfterJaccard.sort()
outfile.write("business_id_1, business_id_2, similarity")
for sItems in candidatesAfterJaccard:
    item = list(sItems)
    outfile.write("\n")
    outfile.write(str(item[0])+","+str(item[1])+","+str(item[2]))




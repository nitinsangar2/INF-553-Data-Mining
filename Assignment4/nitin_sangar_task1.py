from pyspark import SparkContext
import math
import time
import sys
start = time.time()
sc = SparkContext('local[*]','nitin_sangar_sc')

thresh = int(sys.argv[1])
inputPath = sys.argv[2]
outPath = sys.argv[3]
outPath1 = sys.argv[4]
outfile = open(outPath, 'w')
outfile1 = open(outPath1, 'w')

data = sc.textFile(inputPath)
first = data.first()
usersPerBusiness = data.filter(lambda x: x!=first).map(lambda x: x.split(",")).map(lambda x: (x[1],x[0])).groupByKey().mapValues(lambda x: list(set(list(x))))

def getCombinations(iterator):
	userList = list(iterator[1])
	userList.sort()
	for i in range(0, len(userList)):
		for j in range(i+1, len(userList)):
			tup = (min(userList[i], userList[j]), max((userList[i], userList[j])))
			yield(tup, 1)

userEdges = usersPerBusiness.flatMap(getCombinations).reduceByKey(lambda x,y : x+y).filter(lambda x: x[1]>=thresh).keys()

print("edges is {}".format(userEdges.count()))
#userAdjList = userEdges.groupByKey().mapValues(lambda x: list(set(list(x)))).collect()

userAdjDict = {}

for user1,user2 in userEdges.collect():
	if user1 not in userAdjDict: 
		userAdjDict[user1] = [user2]
	else:
		userAdjDict[user1] += [user2]

	if user2 not in userAdjDict: 
		userAdjDict[user2] = [user1]
	else:
		userAdjDict[user2] += [user1]

userAdjDictOriginal = userAdjDict
nodes = list(userAdjDict.keys())
print("length of nodes is {}".format(len(nodes)))

def dfs(node, visited, comp):
	comp.append(node)
	visited.add(node)
	adList = userAdjDict[node]
	for v in adList:
		if v not in visited:
			dfs(v, visited, comp)

def getConnectedComponents():
	components = []
	comp = []
	visited = set()
	count = 0
	for node in nodes:
		if node not in visited:
			count += 1
			if len(comp) != 0:
				comp.sort()
				components.append(comp)
				comp = []
			dfs(node, visited, comp)
	if len(comp) != 0:
		comp.sort()
		components.append(comp)
	return components

def getnode(iterator):
	yield(iterator[0],1)
	yield(iterator[1],1)

def calculateBetweenness(iterator):
	resultSet = set()
	source = iterator
	bfs = []
	visited = set()
	level = {}
	parent = {}
	dist = {}
	paths = {}
	bfs.append(source)
	dist[source] = 0
	paths[source] = 1
	level[source] = 0
	visited.add(source)
	i = 0;
	while i < len(bfs):
		u = bfs[i]
		adjList = userAdjDict[u]
		for v in adjList:
			if v not in visited:
				dist[v] = dist[u] + 1
				level[v] = level[u]+1
				parent[v] = [u]
				paths[v] = paths[u]
				visited.add(v)
				bfs.append(v)
			else:
				if dist[v] > (dist[u]+1):
					dist[v] = dist[u]+1
					paths[v] = paths[u]
				elif dist[v] == dist[u]+1:
					paths[v] += paths[u]
				if level[v] == level[u]+1:
					parent[v] += [u]
		i += 1

	bfs.reverse()
	n = len(bfs)
	# print("length of bfs is {}".format(len(bfs)))
	credit = {}
	for i in range(0,n-1):
		node = bfs[i]
		if node not in credit:
			credit[node] = 1.0
		if node in parent:
			totalPaths = 0
			for p in parent[node]:
				totalPaths += paths[p]
			for p in parent[node]:
				if p not in credit:
					credit[p] = 1.0
				childCredit = float((float(paths[p])/float(totalPaths))*credit[node])
				credit[p] += childCredit
				key = (min(node, p), max(node, p))
				val = float(childCredit)/float(2.0)
				yield(key, val)

users = userEdges.flatMap(getnode).reduceByKey(lambda x,y:x).keys()

userBetweenness = users.flatMap(calculateBetweenness).reduceByKey(lambda x,y: x+y).sortBy(lambda x: (-x[1], x[0][0], x[0][1])).collect()
length = len(userBetweenness)

for i in range(0,length):
	outfile.write(str(userBetweenness[i][0]) + ", ")
	outfile.write(str(userBetweenness[i][1]))
	if i != length-1:
		outfile.write("\n")


print("Count is {}".format(len(userBetweenness)))

degreeDict = {}


totalEdges = 0
for user,edges in userAdjDict.items():
	degreeDict[user] = len(edges)
	totalEdges += degreeDict[user]

print("Total Edges is {}".format(totalEdges))
connectedComponents = getConnectedComponents()

def getModularity(components):
	mod = 0.0
	for comp in components:
		for u in comp:
			for v in comp:
				if v in userAdjDictOriginal[u] or u in userAdjDictOriginal[v]:
					adj = 1
				else:
					adj = 0
				mod += float(adj - (float(degreeDict[u]*degreeDict[v])/float(2*length)))
	mod /= float(2*length)
	return mod

modularity = getModularity(connectedComponents)
print("length of connected components is {}".format(len(connectedComponents)))
print("modularity from function comes out to be {}".format(getModularity(connectedComponents)))

def dfsUtil(u, v, visited):
	if u == v:
		return True
	visited.add(u)
	adList = userAdjDict[u]
	for node in adList:
		if node not in visited:
			if dfsUtil(node, v, visited) == True:
				return True
	return False

maxModularity = modularity
finalComponents = connectedComponents
i = 0
while 2>1:
	userBetweenness = users.flatMap(calculateBetweenness).reduceByKey(lambda x,y: x+y).sortBy(lambda x: (-x[1], x[0][0])).collect()
	if len(userBetweenness) == 0:
		break
	u = userBetweenness[0][0][0]
	v = userBetweenness[0][0][1]
	#print("u is {}".format(u))
	#print("v is {}".format(v))
	if u not in userAdjDict[v] and v not in userAdjDict[u]:
		# All edges have been checked
		break
	userAdjDict[u].remove(v)
	userAdjDict[v].remove(u)

	visited = set()
	if dfsUtil(u, v, visited):
		continue
	
	connectedComponents = getConnectedComponents()
	modularity = getModularity(connectedComponents)

	#print("Connected components become {}".format(len(connectedComponents)))
	#print("modularity becomes {}".format(modularity))
	if modularity >= maxModularity:
		maxModularity = modularity
		finalComponents = connectedComponents


#print("length of connected components is {}".format(len(finalComponents)))
finalComponents.sort(key=lambda x: (len(x), x[0]))
compLen = len(finalComponents)
for i in range(0,compLen):
	eLength = len(finalComponents[i])
	for j in range(0, eLength):
		outfile1.write("'"+finalComponents[i][j]+"'")
		if j != eLength-1:
			outfile1.write(", ")
	if i != compLen-1:
		outfile1.write("\n")
print("Duration : {}".format(time.time()-start))
	



import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import *
from operator import itemgetter
import numpy as np

def getRecords(table,userid):
	items = table.scan(Select= 'ALL_ATTRIBUTES',FilterExpression=Attr('userid').eq(userid))
	return items['Items']

def getRecordsWTD(table,userid,timestamp,device):
	items = table.scan(Select= 'ALL_ATTRIBUTES',FilterExpression=Attr('userid').eq(userid) & Attr('timestamp').gt(timestamp) & Attr('device').eq(device))
	return items['Items']

def getAllRecords(table):
	items = table.scan(Select='ALL_ATTRIBUTES')
	return items['Items']

def solve2V2D3E(values):
	x1=values[0][0]
	y1=values[0][1]
	a=values[1]
	x2=values[2][0]
	y2=values[2][1]
	b=values[3]
	x3=values[4][0]
	y3=values[4][1]
	c=values[5]

	n1=2*(x2-x1)
	n2=2*(y2-y1)
	n3=a-b+(x2**2 + y2**2) - (x1**2 + y1**2)
	n4=2*(x3-x2)
	n5=2*(y3-y2)
	n6=b-c+(x3**2 + y3**2) - (x2**2 + y2**2)

	A=np.array([[n1,n2],[n4,n5]])
	B=np.array([[n3],[n6]])

	ans=np.linalg.inv(A) @ B

	return (ans[0][0],ans[1][0])


def calculateCoordinates(records):
	values = list();
	if len(records)==3:
		for record in records:
			for device in devices:
				if device['device'] == record['device']:
					values.append((int(device['x']),int(device['y'])))
					values.append(float(record['distance'])/convertionFactor)
		
		ans = solve2V2D3E(values)
		return ans

	elif len(records)!=3:
		return (0,0)

def maxTS(records):
	max=0
	for record in records:
		if(max<record['timestamp']):
			max=record['timestamp']

	return int(max)

def updateUserLocation(table,userid,x,y):
	table.update_item(Key={'userid':userid},
		UpdateExpression="SET x=:u, y=:t",
		ExpressionAttributeValues={
                    ":u": Decimal(str(x)),
                    ":t": Decimal(str(y))
                })
	print('Update Done')

convertionFactor = 1/20;

userids = list()
devices = list()
useridWithTime = dict()
dynamodb = boto3.resource('dynamodb','REGION')
user_location = dynamodb.Table('user_location')
location_logs = dynamodb.Table('location_logs')
device_list = dynamodb.Table('device_list')

for device in getAllRecords(device_list):
	devices.append(device)

for item in getAllRecords(user_location):
	userid = item['userid']
	userids.append(userid)
	useridWithTime[userid]=0;

print(useridWithTime)

while True:
	for userid in userids:
		calculationrecords = list()
		for device in devices:
			records = getRecordsWTD(location_logs,userid,useridWithTime[userid],device['device'])
			records = sorted(records, key=itemgetter('timestamp'))
			if len(records) != 0: 
				calculationrecords.append(records[0])
		(x,y) = calculateCoordinates(calculationrecords)
		if x!=0 and y!=0:
			max=maxTS(calculationrecords)
			if max!=0:
				useridWithTime[userid]=max
				updateUserLocation(user_location,userid,x,y);
		print(useridWithTime)

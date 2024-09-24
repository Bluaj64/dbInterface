import mysql.connector
from time import sleep
from Handlers import *

#LoRaWAN Gateway Internal MySQL Database Information
dbIP = "192.168.0.242"
dbUSER = "loradev"
dbPASSWORD = "loradev"

lastID = 0

#LoraWAN Transceiver Bindinds, ties Mote EUI to Application EUI, defined through updateMoteBindings()
Dict_MoteBindings = {}

#Pass to Handlers.py
def handleData(dP):
	if Dict_MoteBindings[dP[1]] in Dict_AppEUI.keys():
		Dict_AppEUI[Dict_MoteBindings[dP[1]]](dP)
	else:
		print("Mote " + str(dP[1]) + " called in using unimplemented AppEUI.")


def getConnection():
	try:
		db = mysql.connector.connect(host=dbIP,user=dbUSER,password=dbPASSWORD)
		return [True,db]
	except:
		return [False,False]

def updateMoteBindings():
	db = getConnection()
	if db[0]:
		c = db[1].cursor()
		c.execute("SELECT * FROM lora_customer.motes;")
		
		Dict_MoteBindings.clear()
		for mote in c.fetchall():
			print("EUI: " + str(mote[0]) + '\tAppEUI: ' + str(mote[1]) + '\tLastRxframe: ' + str(mote[2]))
			Dict_MoteBindings[mote[0]]=mote[1]

def getLatestID():
	db = getConnection()
	if db[0]:
		c = db[1].cursor()
		c.execute("SELECT MAX(id) FROM lora_customer.appdata;")
		
		return c.fetchall()[0][0]
	return 0

def pullLatestData():
	global lastID
	latestID = getLatestID()
	if latestID > lastID:
		db = getConnection()
		if db[0]:
			c = db[1].cursor()
			c.execute("SELECT * FROM lora_customer.appdata WHERE id > {id};".format(id=lastID))
			qResults = c.fetchall()
			print("Num new points: " + str(len(qResults)))
			lastID = latestID
			return qResults
	return []

updateMoteBindings()
print(Dict_MoteBindings)

def doTheStuff():
	global lastID
	with open("STATE","r") as f:
		lastID = int(f.readlines()[0])
		
	while True:
		try:
			for data in pullLatestData():
				handleData(data)
				print(str(data[0]) + '/' + str(lastID))
			print("Handled Data, sleeping for 10 seconds.")
			with open("STATE",'w+') as f:
				f.write(str(lastID))
			sleep(10)
		except:
			with open("STATE","w+") as f:
				f.write(str(lastID))
			sleep(10)

doTheStuff()
#Add handlers for specific AppEUIs here
#Update Dict_AppEUI to include the AppEUI/Function pair

#dP comes in as:
#[0:id,1:mote,2:time,3:time_usec,4:accurateTime,5:seqNo,6:port,7:data]
from influxdb import InfluxDBClient

anemo_init = False
anemo_bindings = {}

gate_init = False
gate_bindings = {}

def Handler_Anemometer(dP):
	try:
		global anemo_init
		if anemo_init == False:
			with open("anemo_motes.csv",'r') as f:
				for i in f.readlines():
					anemo_bindings[i.split(',')[0]] = i.split(',')[1].split('\n')[0]
				anemo_init = True
				print(anemo_bindings)
		print("Anemo " + str(anemo_bindings[str(dP[1])]) + " called in")
		if "TEST" in str(anemo_bindings[str(dP[1])]):
			return
		anemo_id = anemo_bindings[str(dP[1])]
		data = dP[7]
		epoch = formattedEpochTimeToEpochTime(data[0:8])
		cn2 = formattedCn2ToCn2(data[8:12])
		battV = -1.0
		bmeArr = [-1.0,-1.0,-1.0]
		if len(data) > 12:
			battV = formattedBattVToBattV(data[12:14])
		if len(data) > 14:
			bmeArr = formattedBMEtoBME(data[14:22])
		print(str(epoch) + " : " + str(cn2))
		uploadAnemoData(epoch,cn2,anemo_id,battV,bmeArr)
	except Exception as e:
		print(e)
		print("issue")



def Handler_FireroadGate(dP):
	global gate_init
	if gate_init == False:
		with open("gate_motes.csv",'r') as f:
			for i in f.readlines():
				gate_bindings[i.split(',')[0]] = i.split(',')[1].split('\n')[0]
			gate_init = True
			print(gate_bindings)
	gate_id = gate_bindings[str(dP[1])]
	gate_time = dP[2]
	gate_timestr = gate_time.strftime("%Y-%m-%dT%H:%M:%SZ")
	#gate_time = gate_time.split(' ')[0] + "T" + gate_time.split(' ')[1] + "Z"
	data = str(bytes.fromhex(dP[7]))[2:-1]
	if "T" in data:
		return
	gate_status = int(data.split(' ')[0])
	dBatt = (int(data.split(' ')[1]) / 1023) * 15.0
	print(gate_id + " : Gate Status = " + data.split(' ')[0] + " | Battery Voltage = " + str(dBatt))
	#gate_id,gate_time,gate_status,gate_battv
	uploadFireroadGateData(gate_id,gate_timestr,gate_status,dBatt)
	
Dict_AppEUI = {	1 : Handler_Anemometer,
		2 : Handler_FireroadGate
		}

def formattedBattVToBattV(fv):
	return int.from_bytes(bytes.fromhex(fv),'big') / 10.0

def formattedBMEtoBME(fv):
	th = fv[0:4]
	ph = fv[4:6]
	rh = fv[6:8]
	
	t_int = int.from_bytes(bytes.fromhex(th),'big')
	p_int = int.from_bytes(bytes.fromhex(ph),'big')
	r_int = int.from_bytes(bytes.fromhex(rh),'big')
	
	tstr = str(t_int)
	t_float = float(tstr[0:3] + '.' + tstr[3:5]) - 273.15
	p_float = 805.0 + p_int
	r_float = (r_int/255.0) * 100.0
	
	return [t_float,p_float,r_float]

#Anemo Functions
def formattedCn2ToCn2(fv):
	v = str(int.from_bytes(bytes.fromhex(fv),'big'))
	return float(v[0] + '.' + v[1] + v[2] + 'e-' + str(10+int(v[3])))

def formattedEpochTimeToEpochTime(v):
	return int.from_bytes(bytes.fromhex(v),'big')

def uploadAnemoData(epoch,cn2,anemo_id,bv=-1.0,bme=[-1.0,-1.0,-1.0]):
	client = InfluxDBClient(host='localhost',port=8086)
	client.switch_database('data_pool_refactor')
	json_body_Anemo = [
	{
	"measurement": "instruments",
	"tags": {
		"instrument": "Anemo",
		"instrument_uid": "Anemo_" + anemo_id
		},
	"time": int(epoch) - (int(epoch) % 60),
	"fields": {
		"Anemo_Cn2": float(cn2)
		}
	}
	]
	if bv != -1.0:
		json_body_Anemo = [
		{
		"measurement": "instruments",
		"tags": {
			"instrument": "Anemo",
			"instrument_uid": "Anemo_" + anemo_id
			},
		"time": int(epoch) - (int(epoch) % 60),
		"fields": {
		"Anemo_Cn2": float(cn2),
		"Anemo_BattV": float(bv),
			}
		}
		]
	if bme[0] != -1.0:
		json_body_Anemo = [
		{
		"measurement": "instruments",
		"tags": {
			"instrument": "Anemo",
			"instrument_uid": "Anemo_" + anemo_id
			},
		"time": int(epoch) - (int(epoch) % 60),
		"fields": {
		"Anemo_Cn2": float(cn2),
		"Anemo_BattV": float(bv),
		"Anemo_BME280_T": float(bme[0]),
		"Anemo_BME280_P": float(bme[1]),
		"Anemo_BME280_RH": float(bme[2])
			}
		}
		]
	print(client.write_points(json_body_Anemo,protocol='json',time_precision='s'))


def uploadFireroadGateData(gate_id,gate_time,gate_status,gate_battv):
	client = InfluxDBClient(host='localhost',port=8086)
	client.switch_database('infrastructure')
	json_body_gate = [
	{
	"measurement": "Infrastructure",
	"tags": {
		"instrument": "Fireroad_Gate",
		"instrument_uid": gate_id
		},
	"time": gate_time,
	"fields": {
		"Gate_Status": int(gate_status),
		"Gate_BattV": float(gate_battv)
		}
	}
	]
	print(client.write_points(json_body_gate,protocol='json',time_precision='s'))
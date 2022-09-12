# DAS5104 - Projeto Integrador (2021.1)
# Autores: Daniel Peretti, Gustavo Vicenzi e Henrique Pamplona

import paho.mqtt.client as mqtt
import json
from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate a Token from the "Tokens Tab" in the UI
token = "eFMlfLfYw3Xy1QkU6rzOPMAL_STmAjr-erOrUeeoHJ4Sq4vZmYevCBM4hyx3b5lxYzuCRMwVNGmt5Ggv37aWZA=="
org = "UFSC"
bucket = "DME"

client = InfluxDBClient(url="http://localhost:8086", token=token)
write_api = client.write_api(write_options=SYNCHRONOUS)
sensor = Point("LMM")

default_topic = "dev/+/+/+/+"

aux_status_ac = {"dev":0.0}

def check_status_ac(data, device):
    new_data = data
    if "MF" in device:
        aux_status_ac[device[-3:]] = data["corrente"]
    if "AC" in device:
        try:
            if aux_status_ac[device[-3:]] >= 0.17:
                new_data["status_ac_dme"] = 1
            else:
                new_data["status_ac_dme"] = 0
        except:
            new_data["status_ac_dme"] = 0          
    return new_data
    


def write_influx(data, device):
    sensor = Point("LMM")
    sensor.tag("device", device)
    
    for key, value in data.items():
        sensor.field(key, float(value))
        
    try:
        write_api.write(bucket, org, sensor)
        print("Dados enviados: " + sensor.to_line_protocol())
    except:
       print("Erro ao conectar ao banco de dados!")
    


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
        # subscribe to the topic "my/test/topic"
        client.subscribe(default_topic)
    else:
        print("Connect returned result code: " + str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    msg_data = msg.payload.decode("utf-8").replace("'",'"')
    try:
    	data = json.loads(msg_data)
    except:
    	return
    print("Received message: " + msg.topic + " -> " + msg.payload.decode("utf-8"))
   
    dev = " ".join(msg.topic.split("/")[2:]).upper()
    new_data = check_status_ac(data,dev)
   
    write_influx(new_data, dev)
    

# create the client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# enable TLS
#client.tls_set(tls_version=mqtt.ssl.PROTOCOL_TLS)

# set username and password
#client.username_pw_set("pi_user", "PI_senha123")

# connect to HiveMQ Cloud on port 8883
#client.connect("3a019bb6bc834e45bbbb4891d8c88979.s2.eu.hivemq.cloud", 8883)
client.connect("localhost", 1883)

# subscribe to the topic "my/test/topic"
#for device in device_list:
#    client.subscribe(device + " envia")


# Roda cliente broker em loop.
client.loop_forever()


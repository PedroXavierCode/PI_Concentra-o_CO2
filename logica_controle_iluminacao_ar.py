# DAS5104 - Projeto Integrador (2021.1)
# Autores: Daniel Peretti, Gustavo Vicenzi e Henrique Pamplona

import paho.mqtt.client as mqtt
import json
import time 

default_topic = "dev/+/+/+/+"
contador = {"disp": [0,0]}
contador_temp = {"disp": [0,0]}
sala_vazia = {"disp": False}

aux_parametros = {"disp" : {}}
timer_temp = 20
timer_off = 60*20 # seconds
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


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
    else:
        print("Connect returned result code: " + str(rc))

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    msg_data = msg.payload.decode("utf-8").replace("'",'"')
    try:
    	data = json.loads(msg_data)
    except:
    	data = json.loads(str(msg.payload)[5:-3])
    print("Mensagem recebida de: " + msg.topic )#+ " -> " + #msg.payload.decode("utf-8"))

    disp = " ".join(msg.topic.split("/")[2:]).upper()
    new_data = check_status_ac(data,disp)
    aux_parametros[disp] = new_data   

    if "AC" in disp:
        controle_iluminacao(msg.topic, disp)
        controle_ac(msg.topic, disp)

def controle_ac(topic, dispositivo):
    topico = topic + "/command"

    temp_ambiente = float(aux_parametros[dispositivo]["temperatura_ambiente"])
    temp_ac = float(aux_parametros[dispositivo]["temperatura_ac"])
    status_ac_dme = int(aux_parametros[dispositivo]["status_ac_dme"])
    try:
        status_sala_vazia = sala_vazia[dispositivo]
    except:
        status_sala_vazia = True

    if ((status_ac_dme == 1) and (status_sala_vazia == True)):
        client.publish(topico, "desligar_ac")
    elif ((status_ac_dme == 1) and trata_contador_temp(dispositivo) == True):
        client.publish(topico, "set_temp_23")


def controle_iluminacao(topico, dispositivo):
    topico = topico + "/command"
    status_sonoff = int(aux_parametros[dispositivo]["status_sonoff"])
    status_presenca = int(aux_parametros[dispositivo]["presenca"])
    if ((status_sonoff == 0) and (status_presenca == 1)):
        #trata_contador(dispositivo, status_presenca)
        print(dispositivo + "sonoff 0 e presenca 1")
        client.publish(topico, "ligar_sonoff")
    elif ((status_sonoff == 1) and (status_presenca == 0)):
        print(dispositivo + "sonoff 1 e presenca 0")
        trata_contador(dispositivo, status_presenca)
        if (sala_vazia[dispositivo] == True):
            client.publish(topico, "desligar_sonoff")
    elif ((status_sonoff == 1) and (status_presenca == 1)):
        print(dispositivo + "sonoff 1 e presenca 1")
        trata_contador(dispositivo, status_presenca)
        
    

def trata_contador(dispositivo, status_presenca):
    global sala_vazia
    
    if dispositivo not in contador:
        contador[dispositivo] = [0,0]
    if dispositivo not in sala_vazia:
        sala_vazia[dispositivo] = False
    
    if ((status_presenca == 0) and (contador[dispositivo][0] == 0)):
        contador[dispositivo] = [1, round(time.time())]
    elif ((status_presenca == 0) and (contador[dispositivo][0] == 1)):
        if ((round(time.time()) - contador[dispositivo][1]) >= timer_off):
            sala_vazia[dispositivo] = True
        else: 
            sala_vazia[dispositivo] = False
    elif (status_presenca == 1):
        contador[dispositivo] = [0, round(time.time())]
        sala_vazia[dispositivo] = False

def trata_contador_temp(dispositivo):
    if dispositivo not in contador_temp:
        contador_temp[dispositivo] = [0,0]

    if contador_temp[dispositivo][0] == 0:
        contador_temp[dispositivo] = [1, round(time.time())]
        return False
    else:
        if ((round(time.time()) - contador_temp[dispositivo][1]) >= timer_temp):
            contador_temp[dispositivo] = [0, round(time.time())]
            return True
        else:
            print("passou pelo contador")
            return False


# create the client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message


# connect to HiveMQ Cloud on port 1883
client.connect("localhost", 1883)

# subscribe to the topics
client.subscribe(default_topic)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
client.loop_forever()

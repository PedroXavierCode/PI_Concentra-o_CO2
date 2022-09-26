import asyncio, json, random
import paho.mqtt.client as mqtt

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--mf", action="store_true", dest="mono")
parser.add_option("--tf", action="store_true", dest="tri")
parser.add_option("--cm", action="store_true", dest="carbon")

(options, args) = parser.parse_args()

def generate_mono_data():
    return "mf", "999", {"potencia":random.random(),"corrente":random.random()}

def generate_tri_data():
    return "tf", "999", {"tensao_1":210 + random.randrange(8),"potencia_1":0.00,"corrente_1":0.00,"tensao_2":217 + random.randrange(5),"potencia_2":0.00,"corrente_2":0.00,"tensao_3":219 + random.randrange(4),"potencia_3":0.00,"corrente_3":0.00}

def generate_carbon_data():
    return "cm", "999", {"CO2": 430 + random.randrange(10), "TempCO2": 25 + random.randrange(3)}

async def runner(generate_data, client):
    while True:
      await asyncio.sleep(1 + random.randrange(3))
      model, serial, value = generate_data()
      topic = f"dev/lmm/esp32/{model}/{serial}"
      payload = json.dumps(value)
      print(model, serial, value)
      client.publish(topic, payload)

async def main():
    tasks = []
    client = mqtt.Client()
    client.connect("localhost", 1883, 60)
    if options.mono:
        print("Simulando Dispositivo MF")
        tasks.append(asyncio.create_task(runner(generate_mono_data, client)))
    if options.tri:
        print("Simulando Dispositivo TF")
        tasks.append(asyncio.create_task(runner(generate_tri_data, client)))
    if options.carbon:
        print("Simulando Dispositivo CM")
        tasks.append(asyncio.create_task(runner(generate_carbon_data, client)))
    for task in tasks:
        await task

asyncio.run(main())

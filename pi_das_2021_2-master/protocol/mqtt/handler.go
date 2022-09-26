package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"

	"middleware/device"
	"middleware/protocol/influx"
	paho "github.com/eclipse/paho.mqtt.golang"
)

var influxModel influx.Model = influx.Get()

var msgPubHandler paho.MessageHandler = func(client paho.Client, msg paho.Message) {
	fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
}

var onSensorData paho.MessageHandler = func(client paho.Client, msg paho.Message) {
	fmt.Printf("Received message from sensor: %s from topic: %s\n", msg.Payload(), msg.Topic())
	var data map[string]interface{}
	message := strings.ReplaceAll(string(msg.Payload()), "'", "\"")
	err := json.Unmarshal([]byte(message), &data)
	if err != nil {
		fmt.Println("Failed to parse")
		return
	}
	topic := msg.Topic()
	elem := strings.Split(topic, "/")
	elem = elem[2:]
	influxModel.PerformPost(strings.ToUpper(strings.Join(elem, " ")), data)
	_ = device.GetOrCreate(elem[0], elem[1], elem[2]) // TODO: Initialize a var
	// TODO: Check message payload to update sensors,
	// dev.UpdateSensor("F", "my_sensor", 1)
}

var connHandler paho.OnConnectHandler = func(client paho.Client) {
	fmt.Println("Connected")
}

var connLostHandler paho.ConnectionLostHandler = func(client paho.Client, err error) {
	fmt.Printf("Connect lost: %v\n", err)
}

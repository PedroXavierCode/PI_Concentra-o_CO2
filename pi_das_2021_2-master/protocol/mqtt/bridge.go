package mqtt

import (
	"fmt"
	"math/rand"
	"strconv"

	"middleware/config"
	paho "github.com/eclipse/paho.mqtt.golang"
)

// Run starts MQTT client
func Run() {
	conf, _ := config.LoadInternal()
	opts := paho.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", conf.MQTT.Host, conf.MQTT.Port))
	opts.SetClientID(conf.MQTT.ID + "_" + strconv.Itoa(rand.Intn(1000)))
	opts.SetDefaultPublishHandler(msgPubHandler)
	opts.OnConnect = connHandler
	opts.OnConnectionLost = connLostHandler
	client := paho.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	sub(client, conf.MQTT.Topic)
}

func sub(client paho.Client, topic []string) {
	for _, t := range topic {
		token := client.Subscribe(t, 1, onSensorData)
		token.Wait()
	}
}

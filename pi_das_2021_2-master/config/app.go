package config

import (
	"bytes"
	"fmt"
	"os"

	"github.com/spf13/viper"
    "path/filepath"
)

// Influx holds all InfluxDB Configuration
type Influx struct {
	IFUser   string `mapstructure:"INFLUXDB_ADMIN_USER"`
	IFPW     string `mapstructure:"INFLUXDB_ADMIN_PASSWORD"`
	IFOrg    string `mapstructure:"INFLUXDB_ORG"`
	IFBucket string `mapstructure:"INFLUXDB_BUCKET"`
	IFToken  string `mapstructure:"TOKEN"`
	IFUrl    string `mapstructure:"URL"`
}

// Type Grafana handles a Grafana configuration
type Grafana struct {
	Host  string   `json:"host"`
	Port  int      `json:"port"`
	Bearer string `json:"bearer"`
}

// MQTT Holds MQTT Configuration
type MQTT struct {
	Host  string   `json:"host"`
	Port  int      `json:"port"`
	Topic []string `json:"topic"`
	ID    string   `json:"id"`
}

// HTTP Holds MQTT Configuration
type HTTP struct {
	Port int `json:"port"`
}

// Internal holds all Application Configuration
type Internal struct {
	MQTT MQTT `json:"mqtt"`
	HTTP HTTP `json:"http"`
	Grafana Grafana `json:"grafana"`
	Template Template `json:"template"`
}

// Template holds all template models
type Template struct {
	Cm string `json:"cm"`
	Mf string `json:"mf"`
	Tf string `json:"tf"`
}

var v1 = viper.New()

func Write(config []byte) error {
	fmt.Println(string(config))
	err := v1.ReadConfig(bytes.NewBuffer(config))
	if err != nil {
		return err
	}
	fmt.Println(v1.AllSettings())
	return v1.WriteConfig()
}

// LoadInternal loads internal configuration
func LoadInternal() (config Internal, err error) {
	ex, err := os.Executable()
    if err != nil {
        panic(err)
    }
  exPath := filepath.Dir(ex)

	v1.AddConfigPath(exPath)
	v1.SetConfigName("app")
	v1.SetConfigType("json")

	v1.AutomaticEnv()

	err = v1.ReadInConfig()
	if err != nil {
		return
	}

	err = v1.Unmarshal(&config)


	if config.Template.Mf == "" {
		mf, _ := os.ReadFile(fmt.Sprintf("%s/protocol/grafana/templates/mf.json", exPath))
		config.Template.Mf = string(mf)
	}
	if config.Template.Tf == "" {
		tf, _ := os.ReadFile(fmt.Sprintf("%s/protocol/grafana/templates/tf.json", exPath))
		config.Template.Tf = string(tf)
	}
	if config.Template.Cm == "" {
		cm, _ := os.ReadFile(fmt.Sprintf("%s/protocol/grafana/templates/cm.json", exPath))
		config.Template.Cm = string(cm)
	}
	return
}

// LoadInflux returns a structure with viper config
func LoadInflux() (config Influx, err error) {
	v := viper.New()
  ex, err := os.Executable()
    if err != nil {
        panic(err)
    }
  exPath := filepath.Dir(ex)


	v.AddConfigPath(exPath)
	v.SetConfigName(".env")
	v.SetConfigType("env")

	v.AutomaticEnv()

	err = v.ReadInConfig()
	if err != nil {
		return
	}

	err = v.Unmarshal(&config)
	return
}

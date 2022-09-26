package grafana

import (
	"bytes"
	"fmt"
	"strings"
	"net/http"
	"time"
	"middleware/config"
	"io"
)

func postToApi(data string, conf config.Internal) (code int, msg string) {
	client := &http.Client{
		Timeout: time.Second * 10,
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:%d/api/dashboards/db", conf.Grafana.Host, conf.Grafana.Port), bytes.NewBuffer([]byte(data)))
	if err != nil {
		fmt.Printf("Got error %s", err.Error())
		return 400, err.Error()
	}
	req.Header.Add("Content-Type", `application/json`)
	req.Header.Add("Authorization", fmt.Sprintf(`Bearer %s`, conf.Grafana.Bearer))
	resp, err := client.Do(req)
	if err != nil {
		panic("Failed to request Grafana API")
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		panic("Failed to open response body")
	}
	return resp.StatusCode, string(respBody)
}

func CreateDashboard(build string, model string, serial string, conf config.Internal) (code int, msg string) {
	fmt.Printf("### createDashboard: Device: %s // %s // %s \n", build, model, serial)
	var content string
	if model == "tf" {
		content = conf.Template.Tf
	} else if model == "mf" {
		content = conf.Template.Mf
	} else if model == "cm" {
		content = conf.Template.Cm
	}
	content = strings.Replace(content, "#DEVMODEL#", model, -1)
	content = strings.Replace(content, "#DEVMODEL_UPPER#", strings.ToUpper(model), -1)
	content = strings.Replace(content, "#SERIAL#", serial, -1)
	content = strings.Replace(content, "#BUILD#", build, -1)
	code, status := postToApi(content, conf)
	return code, status
}

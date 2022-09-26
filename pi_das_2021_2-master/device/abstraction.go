package device

import (
	"encoding/json"
	"gorm.io/gorm"
	"gorm.io/driver/sqlite"
	"middleware/protocol/http"
	"middleware/config"
	"fmt"
	"os"
    "path/filepath"
)

type Sensor struct {
	gorm.Model
	Measure string `json:"measure"`
	Name string `json:"name"`
	Value float64 `json:"value"`
	DeviceID uint `json:"id"`
}

// TODO: Create Migrator
type Device struct {
	gorm.Model
	Build string `json:"build"`
	DevModel string `json:"devmodel"`
	Serial string `json:"serial"`
	Sensors []Sensor`json:"sensors"`
	StaticDash string `json:"staticDash"`
	DynamicDash string `json:"dinamicDash"`
}


func (s *Device) UpdateSensor(unit, name string, value float64) {
	sensor := Sensor{Measure: unit, Name: name, Value: value, DeviceID: s.ID}
	_ = db.FirstOrCreate(&sensor, "device_id = ? AND name = ? AND measure = ?", s.ID, name, unit)
	db.Model(&sensor).Update("Value", value)
}

func (s *Device) GenerateDash(conf config.Internal) (code int, msg string) {
	code, status := grafana.CreateDashboard(s.Build, s.DevModel, s.Serial, conf)
	return code, status
}

var db *gorm.DB = initModule()

var cache map[uint]interface{} = make(map[uint]interface{})

func initModule() *gorm.DB{
	ex, err := os.Executable()
    if err != nil {
        panic(err)
    }
    exPath := filepath.Dir(ex)

	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("%s/devices.db", exPath)), &gorm.Config{})
	if err != nil {
		panic("Failed to connect database")
	}
	err = db.AutoMigrate(&Device{}, &Sensor{})
	if err != nil {
		panic("Failed to migrate database")
	}
	var devices []Device
	_ = db.Find(&devices)
	for _, dev := range(devices) {
		cache[dev.ID] = dev
	}
	return db
}

func GetByID(id uint) Device {
	var device Device
	db.Find(&device, id)
	return device
}

func GetOrCreate(build string, devmodel string, serial string) Device{
	device := Device{Build: build, DevModel: devmodel, Serial: serial}
	_ = db.FirstOrCreate(&device, "build = ? AND dev_model = ? AND serial = ?", build, devmodel, serial)
	cache[device.ID] = &device
	return device
}

func GetAll() string {
	cacheJson, err := json.Marshal(cache)
	if err != nil {
		return ""
	}
	return string(cacheJson)
}

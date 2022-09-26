package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"strconv"

	"middleware/config"
	"middleware/device"
	"middleware/protocol/mqtt"
	"github.com/gofiber/fiber/v2"
)

var version string // set by the compiler!

func startAsyncServices() {
	go mqtt.Run()
}

func main() {
	verP := flag.Bool("version", false, "show version")
	devP := flag.Bool("dev", false, "run dev mode")
	flag.Parse()
	if *verP {
		fmt.Println(version)
		return
	}
	conf, err := config.LoadInternal()
	if err != nil {
		panic("Failed to read configuration file")
	}
	app := fiber.New()
	app.Static("/", "./web/public")
	app.Get("/api/config", func(c *fiber.Ctx) error {
		err := c.Status(200).JSON(conf)
		return err
	})
	app.Post("/api/config", func(c *fiber.Ctx) error {
		newConf := new(config.Internal)
		if err := json.Unmarshal(c.Body(), &newConf); err != nil {
			fmt.Println(err.Error())
			return err
		}
		conf = *newConf
		err := config.Write(c.Body())
		if err != nil {
			fmt.Println(err.Error())
		}
		return err
	})
	app.Get("/api/device", func(c *fiber.Ctx) error {
		result := device.GetAll()
		err := c.Status(200).JSON(result)
		return err
	})
	app.Post("/api/device/:id/dash", func(c *fiber.Ctx) error {
		id := c.Params("id")
		u64, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			err = c.Status(404).Send([]byte("NOK"))
			return err
		}
		device := device.GetByID(uint(u64))
		code, status := device.GenerateDash(conf)
		fmt.Println(strconv.Itoa(code)+"-"+status)
		err = c.Status(200).Send([]byte(status))
		return err
	})
	app.Get("*", func(c *fiber.Ctx) error {
		err := c.SendFile("web/public/index.html")
		return err
	})
	if !*devP{
		startAsyncServices()
	}
	fmt.Println(app.Listen(":" + strconv.Itoa(conf.HTTP.Port)))
}

native_run:
	go run middleware.go

native_build:
	go build -ldflags "-X main.version=$(shell git describe --tags)" middleware.go

docker_build: native_build
	docker build . -t middleware:latest

docker_run:
	docker-compose up
	
start:
	systemctl start middleware-influx
	systemctl start middleware-core

restart:
	systemctl restart middleware-influx
	systemctl restart middleware-core

services:
	cp ./system-services/* /etc/systemd/system/
	mkdir /web
	cp -r web/public /web
	cp web/src/Images/iot.png web/public
	systemctl enable middleware-influx
	systemctl enable middleware-core

//main.go

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/hhiden/urban-kafka/pkg/config"
	"github.com/knative/pkg/cloudevents"
	"github.com/sacOO7/gowebsocket"
)

type UOMessage1 struct {
	Signal int64
}

type UOMessage2 struct {
	Signal int64
	Data   struct {
		Feed struct {
			Metric string
		}
		Entity struct {
			Name string
			Meta struct {
				Building      string
				BuildingFloor string
			}
		}
		Timeseries struct {
			Unit  string
			Value struct {
				Time         string
				TimeAccuracy float64
				Data         float64
				Type         string
			}
		}
	}
}

type UOOutMessage struct {
	Building string
	Floor    string
	Sensor   string
	Value    float64
}

var (
	connected bool = false
)

func main() {

	config := config.GetConfig()

	log.Println(config.BootStrapServers)

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{config.BootStrapServers}, kafkaConfig)

	if err != nil {
		log.Println("Cannot create producer: " + err.Error())
	} else {
		connected = true
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	socket := gowebsocket.New("wss://api.usb.urbanobservatory.ac.uk/stream")

	socket.OnConnectError = func(err error, socket gowebsocket.Socket) {
		log.Fatal("Received connect error - ", err)
	}

	socket.OnConnected = func(socket gowebsocket.Socket) {
		log.Println("Connected to server")
	}

	socket.OnTextMessage = func(message string, socket gowebsocket.Socket) {
		data := []byte(message)
		var uom UOMessage1
		var uom2 UOMessage2
		json.Unmarshal(data, &uom)
		if uom.Signal == 2 {
			// Data present
			json.Unmarshal(data, &uom2)

			// Pull out the bits of interest
			var outMessage UOOutMessage
			outMessage.Building = uom2.Data.Entity.Meta.Building
			outMessage.Floor = uom2.Data.Entity.Meta.BuildingFloor
			outMessage.Sensor = uom2.Data.Feed.Metric
			outMessage.Value = uom2.Data.Timeseries.Value.Data

			// Convert to JSON
			b, err := json.Marshal(outMessage)
			if err != nil {
				fmt.Println("error:", err)
			} else {
				// Send it as a CloudEvent
				os.Stdout.Write(b)
				os.Stdout.WriteString("\n")
				s := string(b)
				if connected {
					producer.SendMessage(&sarama.ProducerMessage{
						Topic: config.KafkaTopic,
						Value: sarama.StringEncoder(s),
					})

				}
			}
		} else {
			// No value
			log.Println(uom.Signal)
		}
	}

	socket.OnPingReceived = func(data string, socket gowebsocket.Socket) {
		log.Println("Received ping - " + data)
	}

	socket.OnPongReceived = func(data string, socket gowebsocket.Socket) {
		log.Println("Received pong - " + data)
	}

	socket.OnDisconnected = func(err error, socket gowebsocket.Socket) {
		log.Println("Disconnected from server ")
		return
	}

	socket.Connect()

	for {
		select {
		case <-interrupt:
			log.Println("interrupt")
			socket.Close()
			return
		}
	}
}

// Creates a CloudEvent Context for a given Kafka ConsumerMessage.
func cloudEventsContext() *cloudevents.EventContext {
	return &cloudevents.EventContext{
		CloudEventsVersion: cloudevents.CloudEventsVersion,
		EventType:          "uk.ac.ncl.urbanobservatory",
		EventID:            uuid.New().String(),
		Source:             "UrbanSciencesBuilding",
		EventTime:          time.Now(),
	}
}

func getEnv(key string, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

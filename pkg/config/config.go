package config

import (
	"os"
	"strings"
)

type config struct {
	BootStrapServers string
	KafkaTopic       string
	LogLevel         string
	LogFormat        string
}

func GetConfig() config {
	return config{
		BootStrapServers: strings.ToLower(getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")),
		KafkaTopic:       strings.ToLower(getEnv("KAFKA_TOPIC", "test")),
		LogLevel:         strings.ToLower(getEnv("LOG_LEVEL", "info")),
		LogFormat:        strings.ToLower(getEnv("LOG_FORMAT", "text")), //cann be text or json
	}
}

func getEnv(key string, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

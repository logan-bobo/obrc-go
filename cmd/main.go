package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/logan-bobo/obrc-go/internal/data"
)

func main() {
	var dataFile string
	flag.StringVar(&dataFile, "data-file", "data/weather_stations.csv", "measurement file for processing")
	flag.Parse()

	file, err := os.Open(dataFile)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer file.Close()

	measurements := data.Process(file)

	keys := make([]string, 0, len(measurements))

	for key := range measurements {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, value := range keys {
		data := measurements[value]
		fmt.Printf("%s;%s\n", value, data)
	}
}

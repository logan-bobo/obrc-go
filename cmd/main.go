package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

type metadata struct {
	cumulativeTotal float64
	count           float64
	min             float64
	max             float64
	rollingMean     float64
}

func NewMetadata(seed float64) *metadata {
	return &metadata{
		cumulativeTotal: seed,
		count:           1,
		min:             seed,
		max:             seed,
		rollingMean:     seed,
	}
}

func (m *metadata) String() string {
	return fmt.Sprintf("%.1f;%.1f;%.1f", m.min, m.rollingMean, m.max)
}

func main() {
	var dataFile string
	flag.StringVar(&dataFile, "data-file", "data/weather_stations.csv", "measurement file for processing")
	flag.Parse()

	measurements := map[string]*metadata{}

	data, err := os.Open(dataFile)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer data.Close()

	scanner := bufio.NewScanner(data)

	for scanner.Scan() {
		line := scanner.Text()

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// ; represents the difference between our region and temprature
		// for example Hamburg;34.2
		parts := strings.Split(line, ";")

		if len(parts) != 2 {
			log.Println(errors.New("invalid temperature measurement length exiting"))
			os.Exit(1)
		}

		region := parts[0]
		temp, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		r, ok := measurements[region]
		if !ok {
			measurements[region] = NewMetadata(temp)
		} else {
			r.cumulativeTotal += temp
			r.count += 1

			if temp < r.min {
				r.min = temp
			}

			if temp > r.max {
				r.max = temp
			}

			mean := r.cumulativeTotal / r.count
			r.rollingMean = mean
		}
	}

	// this is ugly but fine for a first pass
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


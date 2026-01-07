package data

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
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

type measurements map[string]*metadata

type chunkedMeassurements []measurements

func (m *metadata) String() string {
	return fmt.Sprintf("%.1f;%.1f;%.1f", m.min, m.rollingMean, m.max)
}

func Process(data *os.File, pc int) (measurements, error) {
	fileStats, err := data.Stat()
	if err != nil {
		log.Println(err)
		return measurements{}, err
	}

	chunker := NewChunker(fileStats.Size(), pc)

	// preallocate the chunked measurementsas this has a 1:1 mapping with
	// the amount of chunks to process
	cm := make(chunkedMeassurements, len(chunker.Chunks))
	for i := range cm {
		cm[i] = make(measurements)
	}

	var wg sync.WaitGroup

	for index, chunk := range chunker.Chunks {
		wg.Add(1)
		go func(index int, chunk ChunkInfo) {
			defer wg.Done()
			measurements := cm[index]

			// gives us the length of the chunk
			bytesToRead := chunk.End - chunk.Start
			buffer := make([]byte, (bytesToRead + 128))

			n, _ := data.ReadAt(buffer, chunk.Start)
			buffer = buffer[:n]

			bytesProcessed := 0

			// we could be in the middle of a line here so we need to read forward
			// this works because we always read forward so the previous chunk
			// would have processed this data.
			if chunk.Start > 0 {
				for i := 0; i < len(buffer); i++ {
					if buffer[i] == '\n' {
						bytesProcessed = i + 1
						break
					}
				}
			}

			for i := bytesProcessed; i < len(buffer); i++ {
				if buffer[i] == '\n' {
					line := string(buffer[bytesProcessed:i])

					if line == "" || strings.HasPrefix(line, "#") {
						bytesProcessed = i + 1
						continue
					}

					if i > int(bytesToRead) {
						processLineData(measurements, line)
						break
					}

					processLineData(measurements, line)

					bytesProcessed = i + 1
				}
			}

			// the last chunk may contain data past the last newline as
			// the file might not end with a new line this would result in
			// data being missed
			if index == len(chunker.Chunks)-1 {
				line := string(buffer[bytesProcessed:])

				if line != "" && !strings.HasPrefix(line, "#") {
					processLineData(measurements, line)
				}
			}
		}(index, chunk)
	}
	wg.Wait()

	return mergeMeasurements(cm), nil
}

func processLineData(measurements measurements, line string) {
	// ; represents the difference between our region and temprature
	// for example Hamburg;34.2
	parts := strings.Split(line, ";")

	if len(parts) != 2 {
		log.Printf("Parts: %v", parts)
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

func mergeMeasurements(cm chunkedMeassurements) measurements {
	totals := measurements{}

	for _, m := range cm {
		for key, value := range m {
			r, ok := totals[key]
			if !ok {
				totals[key] = value
			} else {
				r.cumulativeTotal += value.cumulativeTotal
				r.count += 1

				if value.min < r.min {
					r.min = value.min
				}

				if value.max > r.max {
					r.max = value.max
				}

				mean := r.cumulativeTotal / r.count
				r.rollingMean = mean
			}
		}
	}

	return totals
}

package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type ResultMap struct {
	Min   float64
	Max   float64
	Count int64
	Sum   float64
}

type WeatherData struct {
	StationName string
	Temperature float64
}

func main() {
	fmt.Println("lets start 1BRC")
	const numThreads = 15
	const maxLineLength = 106
	args := os.Args
	if len(args) < 2 {
		panic("filename not provided!")
	}

	filename := args[1]
	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}

	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	chunkSize := fileSize / int64(numThreads)
	fmt.Println("Current chunk size is", chunkSize)
	defer file.Close()
	var chunks []int64
	offset := int64(0)
	for {
		offset += chunkSize
		if offset >= fileSize {
			chunks = append(chunks, fileSize)
			break
		}

		file.Seek(offset, 0)
		// Note: always creating the buffer
		buffer := make([]byte, maxLineLength)

		bytesRead, err := file.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			break
		}

		newLinePos := bytes.IndexByte(buffer[:bytesRead], '\n')

		if newLinePos == -1 {
			chunks = append(chunks, fileSize)
			break
		} else {
			offset += int64(newLinePos) + 1
			chunks = append(chunks, offset)
		}
	}

	// Create a wait group to synchronize go routines
	var wg sync.WaitGroup
	ch := make(chan map[string]ResultMap, len(chunks))

	for i := 0; i < len(chunks); i++ {
		var startPos int64
		if i == 0 {
			startPos = int64(0)
		} else {
			startPos = chunks[i-1]
		}

		endPos := chunks[i]

		// Increment the waitgroup counter
		wg.Add(1)

		// Spawn goroutine to process the chunk
		go processFileChunk(ch, &wg, filename, startPos, endPos)
	}

	// wait for all the go routines to finish
	go func() {
		wg.Wait()
		close(ch) // Close the channel after all workers are done
	}()

	// finalResult map
	finalResult := make(map[string]ResultMap)
	for chunkedMap := range ch {
		for key, value := range chunkedMap {
			existingValue, ok := finalResult[key]
			if ok {
				existingValue.Min = math.Min(existingValue.Min, value.Min)
				existingValue.Max = math.Max(existingValue.Max, value.Max)
				existingValue.Count += 1
				existingValue.Sum += value.Sum
			} else {
				existingValue = value
			}

			finalResult[key] = existingValue
		}
	}

	printResults(finalResult)
}

func processFileChunk(ch chan<- map[string]ResultMap, wg *sync.WaitGroup, filePath string, startPos, endPos int64) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	defer wg.Done()

	// Move file pointer to start position
	_, err = file.Seek(startPos, 0)
	if err != nil {
		fmt.Println("Error seeking file:", err)
		return
	}

	// resultant map
	resultMap := make(map[string]ResultMap)
	chunkData := make([]byte, endPos-startPos)
	pos, err := file.Read(chunkData)
	if err != nil {
		fmt.Println("Error reading the chunk")
		return
	}

	data := string(chunkData[:pos])
	lines := strings.Split(data, "\n")

	for _, line := range lines {
		if !strings.ContainsRune(line, ';') {
			continue
		}
		weatherData := parseWeatherData(line)
		existingValue, ok := resultMap[weatherData.StationName]
		if ok {
			existingValue.Count += 1
			existingValue.Min = math.Min(weatherData.Temperature, existingValue.Min)
			existingValue.Max = math.Max(weatherData.Temperature, existingValue.Max)
			existingValue.Sum += weatherData.Temperature
			resultMap[weatherData.StationName] = existingValue
		} else {
			resultMap[weatherData.StationName] = ResultMap{
				Min:   weatherData.Temperature,
				Max:   weatherData.Temperature,
				Count: 1,
				Sum:   weatherData.Temperature,
			}
		}

		// fmt.Printf("A chunk of size %d processed\n", len(resultMap))
	}

	ch <- resultMap

}

func parseWeatherData(line string) WeatherData {
	parts := strings.Split(line, ";")
	if len(parts) < 2 {
		fmt.Println("This causes error:", parts)
		panic(parts)
	}
	// fmt.Println("Float part:", parts[1])
	// Parse the string to a float64
	f, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		panic(err)
	}

	return WeatherData{
		StationName: parts[0],
		Temperature: f,
	}
}

func printResults(finalResult map[string]ResultMap) {
	keys := make([]string, 0, len(finalResult))
	for key := range finalResult {
		keys = append(keys, key)
	}

	// Sort the keys
	sort.Strings(keys)

	fmt.Print("{")
	for i, key := range keys {
		if i > 0 {
			fmt.Print(", ")
		}
		result := finalResult[key]
		fmt.Printf("%s=", key)
		fmt.Printf("%.1f/", result.Min)
		fmt.Printf("%.1f/", result.Sum/float64(result.Count))
		fmt.Printf("%.1f", result.Max)
	}
}

package main

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

func main() {
	fmt.Println("hello world")
	numThreads := 4
	args := os.Args
	if len(args) < 2 {
		panic("filename not provided!")
	}

	filename := args[1]
	fmt.Printf("Filename = %s", filename)
	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}

	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	chunkSize := fileSize / int64(numThreads)
	defer file.Close()

	// Create a wait group to synchronize go routines
	var wg sync.WaitGroup

	for i := 0; i < numThreads; i++ {
		startPos := int64(i) * chunkSize
		endPos := startPos + chunkSize

		if i == numThreads-1 {
			//// Last chunk may be larger if fileSize is not evenly divisible by numThreads
			endPos = fileSize
		}

		// Increment the waitgroup counter
		wg.Add(1)

		// Spawn goroutine to process the chunk
		go func(start, end, threadId int64) {
			defer wg.Done()
			processFileChunk(filename, start, end, threadId)
		}(startPos, endPos, int64(i))

		// wait for all the go routines to finish
		wg.Wait()
	}
	// Creat a scanner to read the file line by line
	scanner := bufio.NewScanner(file)

	// for scanner.Scan() {
	// 	line := scanner.Text()
	// 	// Process the line here
	// 	fmt.Println(line)
	// }

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file: ", err)
	}
}

func processFileChunk(filePath string, startPos, endPos, threadId int64) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	// Move file pointer to start position
	_, err = file.Seek(startPos, 0)
	if err != nil {
		fmt.Println("Error seeking file: ", err)
		return
	}

	// Create a scanner for this section
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// Read line until end position or EOF
		if pos, _ := file.Seek(0, os.SEEK_CUR); pos > endPos {
			break
		}

		line := scanner.Text()
		// Just print the line
		fmt.Println(threadId, line)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file: ", err)
	}
}

package solutions

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

func Naive() error {
	args := os.Args
	if len(args) < 2 {
		panic("filename not provided!")
	}

	filename := args[1]

	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}

	defer file.Close()

	// finalResult map
	finalResult := make(map[string]ResultMap)

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		station, tempStr, hasSemi := strings.Cut(line, ";")
		if !hasSemi {
			continue
		}

		f, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			return err
		}

		existingValue, ok := finalResult[station]

		if ok {
			existingValue.Min = math.Min(existingValue.Min, f)
			existingValue.Max = math.Max(existingValue.Max, f)
			existingValue.Count += 1
			existingValue.Sum += f
		} else {
			existingValue.Min = f
			existingValue.Max = f
			existingValue.Count = 1
			existingValue.Sum = f
		}

		finalResult[station] = existingValue
	}

	stations := make([]string, 0, len(finalResult))
	for station := range finalResult {
		stations = append(stations, station)
	}

	sort.Strings(stations)

	fmt.Print("{")
	for i, station := range stations {
		if i > 0 {
			fmt.Print(", ")
		}
		result := finalResult[station]
		fmt.Printf("%s=", station)
		fmt.Printf("%.1f/", result.Min)
		fmt.Printf("%.1f/", result.Sum/float64(result.Count))
		fmt.Printf("%.1f", result.Max)
	}

	fmt.Print("}\n")
	return nil
}

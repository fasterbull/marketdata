package marketdata

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
  "strings"
	"os"
	"strconv"
  "errors"
 )

type CsvLoader struct {
   RootDataPath string
   TickerFileNamePattern string
   EventFileNamePattern string
}

const tickerFolder = "ticker"
const eventFolder = "event"

func (csvLoader CsvLoader) LoadTickerData(ticker *Ticker) (TickerData, error) {
   var tickerData TickerData
   fileName := getTickerDataFileName(csvLoader.TickerFileNamePattern, ticker.Symbol, ticker.TimeFrame[0])
   fmt.Printf("Filename is %v", fileName)
   filePath := csvLoader.RootDataPath + "\\" + tickerFolder + "\\" + fileName
    fmt.Printf("File is %v", filePath)
		f, err := os.Open(filePath)
    if err != nil {
      return tickerData, err
    }
		// Create a new reader.
		r := csv.NewReader(bufio.NewReader(f))

		result, err := r.ReadAll()

		dataLength := len(result)
		arraySize := dataLength - 1
    fmt.Printf("array size %v", arraySize)
		tickerData.Date = make([]string, arraySize)
		tickerData.Open = make([]float64, arraySize)
		tickerData.High = make([]float64, arraySize)
		tickerData.Low = make([]float64, arraySize)
		tickerData.Close = make([]float64, arraySize)
		tickerData.Volume = make([]int64, arraySize)
    
    headerColumns := []string{"date", "open", "high", "low", "close", "volume"}
    headerMap, err := getColumnPositions(result[0], headerColumns)
    if err != nil {
      return tickerData, err
    }

		for i := 1; i < dataLength; i++ {
			index := i - 1

      tickerData.Date[index] = result[i][headerMap["date"]]
			tickerData.Open[index], err = strconv.ParseFloat(result[i][headerMap["open"]], 64)
			tickerData.High[index], err = strconv.ParseFloat(result[i][headerMap["high"]], 64)
			tickerData.Low[index], err = strconv.ParseFloat(result[i][headerMap["low"]], 64)
			tickerData.Close[index], err = strconv.ParseFloat(result[i][headerMap["close"]], 64)
			tickerData.Volume[index], err = strconv.ParseInt(result[i][headerMap["volume"]], 10, 64)
		  if err != nil {
				return tickerData, err
			}
		}
    fmt.Printf("Loaded!")
    return tickerData, nil
	}

  func (csvLoader CsvLoader) LoadEventData(event *Event) (EventData, error) {
   var eventData EventData
   eventData.Date = make(map[string]bool)
   fileName := getEventDataFileName(csvLoader.EventFileNamePattern, event.Name)
   filePath := csvLoader.RootDataPath + "\\" + eventFolder + "\\" + fileName
		f, err := os.Open(filePath)
    if err != nil {
      return eventData, err
    }

		// Create a new reader.
		r := csv.NewReader(bufio.NewReader(f))
		result, err := r.ReadAll()	

		dataLength := len(result)

    fmt.Printf("Length is %v", dataLength)

		for i := 1; i < dataLength; i++ {
      eventData.Date[result[i][0]] = true
		  if err == io.EOF {
				break
			}
		}
    fmt.Printf("Loaded!")
    return eventData, nil
	}

  func getTickerDataFileName(tickerFileNamePattern string, tickerSymbol string, timeFrame string) string {
    fileName := strings.Replace(tickerFileNamePattern, "{ticker}", tickerSymbol, -1)
    fileName = strings.Replace(fileName, "{timeframe}", timeFrame, -1)

    return fileName
  }

  func getEventDataFileName(eventFileNamePattern string, eventName string) string {
    fileName := strings.Replace(eventFileNamePattern, "{eventname}", eventName, -1)
    
    return fileName
  }

  func getColumnPositions(header []string, expectedValues []string) (map[string]int, error) {
    arrayLength := len(header)
    headerMap := map[string]int{}
    for i := 0; i < arrayLength; i++ {
      headerMap[strings.ToLower(header[i])] = i
    }
    
    return headerMap, validateCsvHeader(headerMap, expectedValues)
  }

  func validateCsvHeader(header map[string]int, expectedValues []string) error {
    if (len(expectedValues) == 0){
      return nil
    }
    
    errMsg := ""
    for _, value := range expectedValues {
        _, exists := header[value]
        if(!exists) {
          if errMsg != "" {
            errMsg = errMsg + ","
          }
          errMsg = errMsg + value
        }
    }

    if errMsg != "" {
      errMsg = "Invalid CSV Header. Should contain: " + errMsg 
      return errors.New(errMsg)
    }
   
    return nil
  }




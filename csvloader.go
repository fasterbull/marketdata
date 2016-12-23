package marketdata

import (
	"bufio"
	"encoding/csv"
	"io"
  "strings"
	"os"
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
   filePath := csvLoader.RootDataPath + "\\" + tickerFolder + "\\" + fileName
		f, err := os.Open(filePath)
    if err != nil {
      return tickerData, errors.New("File Open Error: " + err.Error())
    }
		r := csv.NewReader(bufio.NewReader(f))

		result, err := r.ReadAll()

		dataLength := len(result)
		arraySize := dataLength - 1
    headerColumns := []string{"id", "date", "open", "high", "low", "close", "volume"}
    header, err := getColumnPositions(result[0], headerColumns)
    if err != nil {
      return tickerData, err
    }

    tickerData.initialize(header, arraySize)

		for i := 1; i < dataLength; i++ {
			index := i - 1
      err := tickerData.add(result[i], header, index)
		  if err != nil {
				return tickerData, err
			}
		}
    return tickerData, nil
	}

  func (csvLoader CsvLoader) LoadEventData(event *Event) (EventData, error) {
   var eventData EventData
   eventData.Date = make(map[string]bool)
   fileName := getEventDataFileName(csvLoader.EventFileNamePattern, event.Name)
   filePath := csvLoader.RootDataPath + "\\" + eventFolder + "\\" + fileName
		f, err := os.Open(filePath)
    if err != nil {
      return eventData, errors.New("File Open Error: " + err.Error())
    }

		r := csv.NewReader(bufio.NewReader(f))
		result, err := r.ReadAll()	
		dataLength := len(result)

    headerColumns := []string{"date"}
    headerMap, err := getColumnPositions(result[0], headerColumns)
    if err != nil {
      return eventData, err
    }

    for i := 1; i < dataLength; i++ {
      eventData.Date[result[i][headerMap["date"]]] = true
		  if err == io.EOF {
				break
			}
		}
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




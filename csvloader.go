package marketdata

import (
	"bufio"
	"encoding/csv"
	"io"
  "strings"
	"os"
  "errors"
  "time"
 )

type CsvLoader struct {
   RootDataPath string
   TickerFileNamePattern string
   EventFileNamePattern string
   DateFormat string
}

type IndexRange struct {
   begin int
   end int
}

const tickerFolder = "ticker"
const eventFolder = "event"

func (csvLoader CsvLoader) LoadTickerData(symbol string, tickerConfig *TickerConfig) (TickerData, error) {
   var tickerData TickerData
   fileName := getTickerDataFileName(csvLoader.TickerFileNamePattern, symbol, tickerConfig.TimeFrame)
   filePath := csvLoader.RootDataPath + "\\" + tickerFolder + "\\" + fileName
		f, err := os.Open(filePath)
    if err != nil {
      return tickerData, errors.New("File Open Error: " + err.Error())
    }
		r := csv.NewReader(bufio.NewReader(f))

		result, err := r.ReadAll()
    header, err := getColumnPositions(result[0], tickerConfig.Fields)
    if err != nil {
      return tickerData, err
    }

    indexRange, _ := getIndexRange(result, header["date"], csvLoader.DateFormat, &tickerConfig.Range)
    tickerData.initialize(header, ((indexRange.end - indexRange.begin) + 1))

    index := -1
		for i := indexRange.begin; i < indexRange.end; i++ {
			index++
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
  
  func getIndexRange(records [][]string, dateColPos int, dateFormat string, dateRange *DateRange) (IndexRange, error) {
    dataLength := len(records)
    var indexRange IndexRange
    var err error
   
    if dateRange == nil {
      indexRange.begin = 1
      indexRange.end = dataLength
    } else {
      startDate, _ := time.Parse(dateFormat, dateRange.StartDate)
      endDate, _ := time.Parse(dateFormat, dateRange.EndDate)
      for i := 1; i < dataLength; i++ {
        date, _ := time.Parse(dateFormat, records[i][dateColPos])
        if indexRange.begin == 0 && (date.Equal(startDate) || date.After(startDate)) {
          indexRange.begin = i
        } else if date.Equal(endDate) || date.After(endDate) {
          indexRange.end = i
          break
        }
      }
      if &indexRange.end == nil && &indexRange.begin != nil {
        indexRange.end = dataLength - 1
      }
    }

    return indexRange, err
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

  func getColumnPositions(header []string, fields []string) (map[string]int, error) {
    arrayLength := len(header)
    headerMap := map[string]int{}
    for i := 0; i < arrayLength; i++ {
      if(len(fields) == 0 || inArray(header[i], fields)) {
        headerMap[strings.ToLower(header[i])] = i
      }
    }
    
    return headerMap, validateCsvHeader(headerMap, fields)
  }

  func inArray(value string, array []string) bool {
     for _, item := range array {
        if value == item {
          return true
          break
        }
     }
     return false
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




package marketdata

import (
	"bufio"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"strings"
	"time"
)

type CsvReader struct {
	TickerDataPath        string
	EventDataPath         string
	TickerFileNamePattern string
	EventFileNamePattern  string
	DividendFileNamePattern string
	SplitFileNamePattern  string
	DateFormat            string
}

type indexRange struct {
	begin int
	end   int
}

func (csvReader CsvReader) ReadDivedendData(symbol string, source string) (TickerDividendData, error) {
	var tickerDd TickerDividendData
	fileName := getFileName(csvReader.DividendFileNamePattern, "{ticker}", symbol)
	filePath := csvReader.TickerDataPath + string(os.PathSeparator) + fileName
	f, err := os.Open(filePath)
	if err != nil {
		return tickerDd, errors.New("File Open Error: " + err.Error())
	}
	r := csv.NewReader(bufio.NewReader(f))
	result, err := r.ReadAll()
	if err != nil {
		return tickerDd, err
	}
	size := getCountOfMatchingItems(result, "DIVIDEND", 0)
	tickerDd.initialize(size)
	return tickerDd, nil
}

func (csvReader CsvReader) ReadTickerData(symbol string, tickerConfig *ReadConfig) (TickerData, error) {
	var tickerData TickerData
	fileName := getTickerDataFileName(csvReader.TickerFileNamePattern, symbol, tickerConfig.TimeFrame)
	filePath := csvReader.TickerDataPath + string(os.PathSeparator) + fileName
	f, err := os.Open(filePath)
	if err != nil {
		return tickerData, errors.New("File Open Error: " + err.Error())
	}
	r := csv.NewReader(bufio.NewReader(f))
	result, err := r.ReadAll()
	if err != nil {
		return tickerData, err
	}
	header, err := getColumnPositions(result[0], tickerConfig.Filter)
	if err != nil {
		return tickerData, err
	}
	indexRange, err := getIndexRange(result, header, csvReader.DateFormat, &tickerConfig.Range)
	if err != nil {
		return tickerData, err
	}
	tickerData.initialize(header, (indexRange.end - indexRange.begin))
	index := -1
	for i := indexRange.begin; i < indexRange.end; i++ {
		index++
		err := tickerData.addFromRecords(result[i], header, index)
		if err != nil {
			return tickerData, err
		}
	}
	return tickerData, nil
}

func (csvReader CsvReader) ReadEventData(event *Event) (EventData, error) {
	var eventData EventData
	eventData.Date = make(map[string]bool)
	fileName := getEventDataFileName(csvReader.EventFileNamePattern, event.Name)
	filePath := csvReader.EventDataPath + string(os.PathSeparator) + fileName
	f, err := os.Open(filePath)
	if err != nil {
		return eventData, errors.New("File Open Error: " + err.Error())
	}
	r := csv.NewReader(bufio.NewReader(f))
	result, err := r.ReadAll()
	if err != nil {
		return eventData, err
	}
	dataLength := len(result)
	header, err := getColumnPositions(result[0], []string{"date"})
	if err != nil {
		return eventData, err
	}
	for i := 1; i < dataLength; i++ {
		eventData.Date[result[i][header["date"]]] = true
		if err == io.EOF {
			break
		}
	}
	return eventData, nil
}
func getCountOfMatchingItems(records [][]string, pattern string, index int) int {
	count := 0
	dataLength := len(records)
	for i := 1; i < dataLength; i++ {
	   if strings.Contains(records[i][index], pattern) {
		   count++
	   }
	}
	return count
}
func getIndexRange(records [][]string, header map[string]int, dateFormat string, dateRange *DateRange) (indexRange, error) {
	dataLength := len(records)
	var indexRange indexRange
	var err error
	if dateRange.StartDate == "" {
		indexRange.begin = 1
		indexRange.end = dataLength
		return indexRange, err
	}
	dateColumnIndex, exists := header["date"]
	if !exists {
		dateColumn, err := getColumnPositions(records[0], []string{"date"})
		if err != nil {
			return indexRange, err
		}
		dateColumnIndex = dateColumn["date"]
	}
	startDate, _ := time.Parse(dateFormat, dateRange.StartDate)
	endDate, _ := time.Parse(dateFormat, dateRange.EndDate)
	for i := 1; i < dataLength; i++ {
		date, _ := time.Parse(dateFormat, records[i][dateColumnIndex])
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

func getFileName(fileNamePattern string, replacePattern string, replaceValue string) string {
	fileName := strings.Replace(fileNamePattern, replacePattern, replaceValue, -1)
	return fileName
}

func getColumnPositions(header []string, filter []string) (map[string]int, error) {
	arrayLength := len(header)
	headerMap := map[string]int{}
	for i := 0; i < arrayLength; i++ {
		if len(filter) == 0 || inArray(header[i], filter) {
			headerMap[strings.ToLower(header[i])] = i
		}
	}
	err := validateCsvHeader(headerMap, filter)
	return headerMap, err
}

func validateCsvHeader(header map[string]int, expectedValues []string) error {
	if len(expectedValues) == 0 {
		return nil
	}
	errMsg := ""
	for _, value := range expectedValues {
		_, exists := header[value]
		if !exists {
			if errMsg != "" {
				errMsg = errMsg + ","
			}
			errMsg = errMsg + value
		}
	}
	if errMsg != "" {
		return errors.New("Invalid CSV Header. Missing header item(s): " + errMsg)
	}
	return nil
}

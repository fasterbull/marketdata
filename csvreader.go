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
	TickerDataPath          string
	EventDataPath           string
	TickerFileNamePattern   string
	EventFileNamePattern    string
	DividendFileNamePattern string
	SplitFileNamePattern    string
	DateFormat              string
}

type indexRange struct {
	begin int
	end   int
}

func (csvReader CsvReader) ReadDividendData(symbol string, source DataSource) (TickerDividendData, error) {
	var tickerDd TickerDividendData
	fileName := getFileName(csvReader.DividendFileNamePattern, "{ticker}", symbol)
	filePath := csvReader.TickerDataPath + string(os.PathSeparator) + fileName
	f, err := os.Open(filePath)
	if err != nil {
		return tickerDd, errors.New("File Open Error: " + err.Error())
	}
	if source == YAHOO {
		r := bufio.NewReader(f)
		err = addFromYahooSplitDivData(&tickerDd, "dividend", r, csvReader.DateFormat)
	} else {
		r := csv.NewReader(bufio.NewReader(f))
		header := make(map[string]int)
		header["date"] = 0
		header["dividend"] = 1
		err = addFromStandardCsvData(&tickerDd, header, r, csvReader.DateFormat)
	}
	return tickerDd, err
}

func (csvReader CsvReader) ReadSplitData(symbol string, source DataSource) (TickerSplitData, error) {
	var tickerSd TickerSplitData
	fileName := getFileName(csvReader.SplitFileNamePattern, "{ticker}", symbol)
	if fileName == "" {
		return tickerSd, errors.New("File for ticker: '" + symbol + "' does not exist.")
	}
	filePath := csvReader.TickerDataPath + string(os.PathSeparator) + fileName
	f, err := os.Open(filePath)
	if err != nil {
		return tickerSd, errors.New("File Open Error: " + err.Error())
	}
	if source == YAHOO {
		r := bufio.NewReader(f)
		err = addFromYahooSplitDivData(&tickerSd, "split", r, csvReader.DateFormat)
	} else {
		r := csv.NewReader(bufio.NewReader(f))
		header := make(map[string]int)
		header["date"] = 0
		header["split"] = 1
		err = addFromStandardCsvData(&tickerSd, header, r, csvReader.GetDateFormat())
	}
	return tickerSd, err
}

func (csvReader CsvReader) GetDateFormat() string {
	return csvReader.DateFormat
}

func addFromYahooSplitDivData(data Data, dataType string, r *bufio.Reader, dateFormat string) error {
	line, err := r.ReadString(10)
	records := [][]string{}
	var splitLine []string
	for err != io.EOF {
		line, err = r.ReadString(10)
		if strings.Contains(strings.ToLower(line), dataType) {
			line = strings.Replace(line, "\n", "", -1)
			line = strings.Replace(line, "\r", "", -1)
			splitLine = strings.Split(line, ",")
			records = append(records, []string{splitLine[1], splitLine[2]})
		}
	}
	header := make(map[string]int)
	header["date"] = 0
	header[dataType] = 1
	size := len(records)
	data.initialize(len(records))
	index := -1
	for i := 0; i < size; i++ {
		index++
		err := data.addFromRecords(records[i], header, index, dateFormat)
		if err != nil {
			return err
		}
	}
	return nil
}

func addFromStandardCsvData(data Data, header map[string]int, r *csv.Reader, dateFormat string) error {
	records, err := r.ReadAll()
	if err != nil {
		return err
	}
	size := len(records)
	data.initialize(size - 1)
	index := -1
	for i := 1; i < size; i++ {
		index++
		err := data.addFromRecords(records[i], header, index, dateFormat)
		if err != nil {
			return err
		}
	}
	return nil
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
	indexRange, err := getIndexRange(result, header, &tickerConfig.Range, csvReader.DateFormat)
	if err != nil {
		return tickerData, err
	}
	tickerData.initialize(header, (indexRange.end - indexRange.begin))
	index := -1
	for i := indexRange.begin; i < indexRange.end; i++ {
		index++
		err := tickerData.addFromRecords(result[i], header, index, csvReader.DateFormat)
		if err != nil {
			return tickerData, err
		}
	}
	return tickerData, nil
}

func (csvReader CsvReader) ReadEventData(event *Event) (EventData, error) {
	var eventData EventData
	eventData.Date = make(map[time.Time]bool)
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
		date, _ := time.Parse(csvReader.DateFormat, result[i][header["date"]])
		eventData.Date[date] = true
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
func getIndexRange(records [][]string, header map[string]int, dateRange *DateRange, dateFormat string) (indexRange, error) {
	dataLength := len(records)
	var indexRange indexRange
	var err error
	if dateRange.StartDate.IsZero() {
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
	for i := 1; i < dataLength; i++ {
		date, _ := time.Parse(dateFormat, records[i][dateColumnIndex])
		if indexRange.begin == 0 && (date.Equal(dateRange.StartDate) || date.After(dateRange.StartDate)) {
			indexRange.begin = i
		} else if date.Equal(dateRange.EndDate) || date.After(dateRange.EndDate) {
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

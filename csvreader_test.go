package marketdata

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func Test_getTickerDataFileName(t *testing.T) {
	result := getTickerDataFileName("{ticker}-{timeframe}.csv", "spy", "daily")
	expectedValue := "spy-daily.csv"
	if result != expectedValue {
		t.Log("Failed to get ticker data filename. Result was: " + result + " but should be: " + expectedValue)
		t.Fail()
	}
}

func Test_getEventDataFileName(t *testing.T) {
	result := getEventDataFileName("{eventname}.csv", "opec-oil-cut")
	expectedValue := "opec-oil-cut.csv"
	if result != expectedValue {
		t.Log("Failed to get event data filename. Result was: " + result + " but should be: " + expectedValue)
		t.Fail()
	}
}

func Test_getColumnPositions(t *testing.T) {
	header := []string{"Date", "Open", "High", "Low", "Close", "Volume"}
	result, _ := getColumnPositions(header, make([]string, 0))
	expectedValue := map[string]int{
		"date":   0,
		"open":   1,
		"high":   2,
		"low":    3,
		"close":  4,
		"volume": 5,
	}
	if (result["date"] != expectedValue["date"]) ||
		(result["open"] != expectedValue["open"]) ||
		(result["high"] != expectedValue["high"]) ||
		(result["low"] != expectedValue["low"]) ||
		(result["close"] != expectedValue["close"]) ||
		(result["volume"] != expectedValue["volume"]) {
		t.Log("Failed to get column positions. Result was: ", result, " but should be:", expectedValue)
		t.Fail()
	}
}

func Test_readYahooDividendData(t *testing.T) {
	var csvReader CsvReader
	csvReader.DataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker"
	csvReader.FileNamePattern = "{ticker}-yahoosplitdividend.csv"
	csvReader.DateFormat = "20060102"
	symbol := "someticker"
	result, err := csvReader.readDividendData(symbol, YAHOO)
	var expectedValue TickerDividendData
	dates := []string{"20050620", "20050324", "20020308", "20011214"}
	expectedValue.Date = createDates(dates, csvReader.DateFormat)
	expectedValue.Amount = []float64{0.146000, 0.274000, 0.057500, 0.135000}
	if !reflect.DeepEqual(result, expectedValue) || err != nil {
		t.Log("Failed ReadYahooDividendData. Result was: ", result, " but should be: ", expectedValue)
		t.Log("Returned error is:", err)
		t.Fail()
	}
}

func Test_readYahooSplitData(t *testing.T) {
	var csvReader CsvReader
	csvReader.DataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker"
	csvReader.FileNamePattern = "{ticker}-yahoosplitdividend.csv"
	csvReader.DateFormat = "20060102"
	symbol := "someticker"
	result, err := csvReader.readSplitData(symbol, YAHOO)
	var expectedValue TickerSplitData
	dates := []string{"20050609", "20020605"}
	expectedValue.Date = createDates(dates, csvReader.DateFormat)
	expectedValue.BeforeSplitQty = []int{1, 2}
	expectedValue.AfterSplitQty = []int{2, 3}
	if !reflect.DeepEqual(result, expectedValue) || err != nil {
		t.Log("Failed readSplitData. Result was: ", result, " but should be: ", expectedValue)
		t.Log("Returned error is:", err)
		t.Fail()
	}
}

func Test_readStandardSplitData(t *testing.T) {
	var csvReader CsvReader
	csvReader.DataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker"
	csvReader.FileNamePattern = "{ticker}-splitdata.csv"
	csvReader.DateFormat = "20060102"
	symbol := "someticker"
	result, err := csvReader.readSplitData(symbol, OTHER)
	var expectedValue TickerSplitData
	dates := []string{"20050609", "20020605"}
	expectedValue.Date = createDates(dates, csvReader.DateFormat)
	expectedValue.BeforeSplitQty = []int{1, 2}
	expectedValue.AfterSplitQty = []int{2, 3}
	if !reflect.DeepEqual(result, expectedValue) || err != nil {
		t.Log("Failed readSplitData. Result was: ", result, " but should be: ", expectedValue)
		t.Log("Returned error is:", err)
		t.Fail()
	}
}

func Test_readStandardDividendData(t *testing.T) {
	var csvReader CsvReader
	csvReader.DataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker"
	csvReader.FileNamePattern = "{ticker}-dividenddata.csv"
	csvReader.DateFormat = "20060102"
	symbol := "someticker"
	result, err := csvReader.readDividendData(symbol, OTHER)
	var expectedValue TickerDividendData
	dates := []string{"20050620", "20050324", "20020308", "20011214"}
	expectedValue.Date = createDates(dates, csvReader.DateFormat)
	expectedValue.Amount = []float64{0.146000, 0.274000, 0.057500, 0.135000}
	if !reflect.DeepEqual(result, expectedValue) || err != nil {
		t.Log("Failed ReadStandardDividendData. Result was: ", result, " but should be: ", expectedValue)
		t.Log("Returned error is:", err)
		t.Fail()
	}
}

func Test_readTickerDataHandlesErrors(t *testing.T) {
	var dateRange DateRange
	filter := []string{"id", "date", "open", "high", "low", "close", "volume"}
	config := ReadConfig{"daily", filter, dateRange}
	testCases := []struct {
		name         string
		symbol       string
		tickerConfig ReadConfig
		errorMsg     string
	}{
		{"fileDoesNotExist", "invalidTicker", config, "File Open Error"},
		{"tickerFileNoHeader", "noheader", config, "Invalid CSV Header. Missing header item(s): id,date,open,high,low,close,volume"},
	}
	var csvReader CsvReader
	csvReader.DataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker"
	csvReader.FileNamePattern = "{ticker}-{timeframe}.csv"
	csvReader.DateFormat = "1/2/2006"
	for _, tc := range testCases {
		_, err := csvReader.readTickerData(tc.symbol, &tc.tickerConfig)
		var expectedError = tc.errorMsg

		if err == nil || !strings.Contains(err.Error(), expectedError) {
			t.Log("ReadTickerData test case ", tc.name, " did not handle invalid ticker file. Error was: ", err, " but should be: ", expectedError)
			t.Fail()
		}
	}
}

func Test_readEventDataHandlesErrors(t *testing.T) {
	testCases := []struct {
		name      string
		eventName string
		errorMsg  string
	}{
		{"fileDoesNotExist", "invalidEvent", "File Open Error"},
		{"eventFileNoHeader", "noheader", "Invalid CSV Header. Missing header item(s): date"},
	}
	var csvReader CsvReader
	csvReader.DataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "event"
	csvReader.FileNamePattern = "{eventname}.csv"
	csvReader.DateFormat = "1/2/2006"
	var event Event
	for _, tc := range testCases {
		event.Name = tc.eventName
		_, err := csvReader.readEventData(&event)
		var expectedError = tc.errorMsg

		if err == nil || !strings.Contains(err.Error(), expectedError) {
			t.Log("readEventData test case ", tc.name, " did not handle invalid event file. Error was: ", err, " but should be: ", expectedError)
			t.Fail()
		}
	}
}

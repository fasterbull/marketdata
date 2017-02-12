package marketdata

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestGetTickerDataFileName(t *testing.T) {
	result := getTickerDataFileName("{ticker}-{timeframe}.csv", "spy", "daily")
	expectedValue := "spy-daily.csv"
	if result != expectedValue {
		t.Log("Failed to get ticker data filename. Result was: " + result + " but should be: " + expectedValue)
		t.Fail()
	}
}

func TestGetEventDataFileName(t *testing.T) {
	result := getEventDataFileName("{eventname}.csv", "opec-oil-cut")
	expectedValue := "opec-oil-cut.csv"
	if result != expectedValue {
		t.Log("Failed to get event data filename. Result was: " + result + " but should be: " + expectedValue)
		t.Fail()
	}
}

func TestGetColumnPositions(t *testing.T) {
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

func TestReadEventData(t *testing.T) {
	var csvReader CsvReader
	csvReader.EventDataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "event"
	csvReader.EventFileNamePattern = "{eventname}.csv"
	csvReader.DateFormat = "1/2/2006"
	var event Event
	event.Name = "testevent"
	result, _ := csvReader.ReadEventData(&event)
	dates := []string{"5/26/2000", "7/11/2000", "9/6/2011"}
	realDates := createDates(dates, csvReader.DateFormat)
	expectedValue := map[time.Time]bool{
		realDates[0]: true,
		realDates[1]: true,
		realDates[2]: true,
	}
	if (result.Date[realDates[0]] != expectedValue[realDates[0]]) ||
		(result.Date[realDates[1]] != expectedValue[realDates[1]]) ||
		(result.Date[realDates[2]] != expectedValue[realDates[2]]) {
		t.Log("Failed Read EventData. Result was: ", result, " but should be: ", expectedValue)
		t.Fail()
	}
}

func TestReadTickerData(t *testing.T) {
	var csvReader CsvReader
	csvReader.TickerDataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker"
	csvReader.TickerFileNamePattern = "{ticker}-{timeframe}.csv"
	csvReader.DateFormat = "20060102"
	symbol := "someticker"
	var dateRange DateRange
	tickerConfig := ReadConfig{"daily", nil, dateRange}
	result, _ := csvReader.ReadTickerData(symbol, &tickerConfig)
	var expectedValue TickerData
	expectedValue.Id = []int32{0, 1, 2}
	dates := []string{"12/7/2016", "12/8/2016", "12/9/2016"}
	expectedValue.Date = createDates(dates, csvReader.DateFormat)
	expectedValue.Open = []float64{134.58, 136.25, 138.39}
	expectedValue.High = []float64{136.17, 138.21, 138.82}
	expectedValue.Low = []float64{134.17, 135.80, 137.75}
	expectedValue.Close = []float64{135.89, 138.03, 138.30}
	expectedValue.Volume = []int64{30859300, 47794400, 34276600}

	if !reflect.DeepEqual(result, expectedValue) {
		t.Log("Failed Read TickerData. Result was: ", result, " but should be: ", expectedValue)
		t.Fail()
	}

}

func TestReadYahooDividendData(t *testing.T) {
	var csvReader CsvReader
	csvReader.TickerDataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker"
	csvReader.DividendFileNamePattern = "{ticker}-yahoosplitdividend.csv"
	csvReader.DateFormat = "20060102"
	symbol := "someticker"
	result, err := csvReader.ReadDividendData(symbol, "yahoo")
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

func TestReadYahooSplitData(t *testing.T) {
	var csvReader CsvReader
	csvReader.TickerDataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker"
	csvReader.SplitFileNamePattern = "{ticker}-yahoosplitdividend.csv"
	csvReader.DateFormat = "20060102"
	symbol := "someticker"
	result, err := csvReader.ReadSplitData(symbol, "yahoo")
	var expectedValue TickerSplitData
	dates := []string{"20050609", "20020605"}
	expectedValue.Date = createDates(dates, csvReader.DateFormat)
	expectedValue.BeforeSplitQty = []int{1, 2}
	expectedValue.AfterSplitQty = []int{2, 3}
	if !reflect.DeepEqual(result, expectedValue) || err != nil {
		t.Log("Failed ReadTickerSplitData. Result was: ", result, " but should be: ", expectedValue)
		t.Log("Returned error is:", err)
		t.Fail()
	}
}

func TestReadStandardSplitData(t *testing.T) {
	var csvReader CsvReader
	csvReader.TickerDataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker"
	csvReader.SplitFileNamePattern = "{ticker}-splitdata.csv"
	csvReader.DateFormat = "20060102"
	symbol := "someticker"
	result, err := csvReader.ReadSplitData(symbol, "")
	var expectedValue TickerSplitData
	dates := []string{"20050609", "20020605"}
	expectedValue.Date = createDates(dates, csvReader.DateFormat)
	expectedValue.BeforeSplitQty = []int{1, 2}
	expectedValue.AfterSplitQty = []int{2, 3}
	if !reflect.DeepEqual(result, expectedValue) || err != nil {
		t.Log("Failed ReadTickerSplitData. Result was: ", result, " but should be: ", expectedValue)
		t.Log("Returned error is:", err)
		t.Fail()
	}
}

func TestReadStandardDividendData(t *testing.T) {
	var csvReader CsvReader
	csvReader.TickerDataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker"
	csvReader.DividendFileNamePattern = "{ticker}-dividenddata.csv"
	csvReader.DateFormat = "20060102"
	symbol := "someticker"
	result, err := csvReader.ReadDividendData(symbol, "")
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

func TestReadTickerDataHandlesErrors(t *testing.T) {
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
	csvReader.TickerDataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker"
	csvReader.TickerFileNamePattern = "{ticker}-{timeframe}.csv"
	csvReader.DateFormat = "1/2/2006"
	for _, tc := range testCases {
		_, err := csvReader.ReadTickerData(tc.symbol, &tc.tickerConfig)
		var expectedError = tc.errorMsg

		if err == nil || !strings.Contains(err.Error(), expectedError) {
			t.Log("ReadTickerData test case ", tc.name, " did not handle invalid ticker file. Error was: ", err, " but should be: ", expectedError)
			t.Fail()
		}
	}
}

func TestReadEventDataHandlesErrors(t *testing.T) {
	testCases := []struct {
		name      string
		eventName string
		errorMsg  string
	}{
		{"fileDoesNotExist", "invalidEvent", "File Open Error"},
		{"eventFileNoHeader", "noheader", "Invalid CSV Header. Missing header item(s): date"},
	}
	var csvReader CsvReader
	csvReader.EventDataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "event"
	csvReader.EventFileNamePattern = "{eventname}.csv"
	csvReader.DateFormat = "1/2/2006"
	var event Event
	for _, tc := range testCases {
		event.Name = tc.eventName
		_, err := csvReader.ReadEventData(&event)
		var expectedError = tc.errorMsg

		if err == nil || !strings.Contains(err.Error(), expectedError) {
			t.Log("ReadEventData test case ", tc.name, " did not handle invalid event file. Error was: ", err, " but should be: ", expectedError)
			t.Fail()
		}
	}
}

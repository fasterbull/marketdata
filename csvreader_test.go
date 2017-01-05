package marketdata

import (
	"reflect"
	"strings"
	"testing"
	"os"
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
	csvReader := CsvReader{"." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker",
		"." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "event",
		                    "{ticker}-{timeframe}.csv", "{eventname}.csv", "1/2/2006"}
	var event Event
	event.Name = "testevent"

	result, _ := csvReader.ReadEventData(&event)
	expectedValue := map[string]bool{
		"5/26/2000": true,
		"7/11/2000": true,
		"9/6/2011":  true,
	}
	if (result.Date["5/26/2000"] != expectedValue["5/26/2000"]) ||
		(result.Date["7/11/2000"] != expectedValue["7/11/2000"]) ||
		(result.Date["9/6/2011"] != expectedValue["9/6/2011"]) {
		t.Log("Failed Read EventData. Result was: ", result, " but should be: ", expectedValue)
		t.Fail()
	}
}

func TestReadTickerData(t *testing.T) {
	csvReader := CsvReader{"." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker",
		"." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "event",
		 "{ticker}-{timeframe}.csv", "{eventname}.csv", "1/2/2006"}
	symbol := "someticker"
	var dateRange DateRange
	tickerConfig := ReadConfig{"daily", nil, dateRange}

	result, _ := csvReader.ReadTickerData(symbol, &tickerConfig)
	var expectedValue TickerData
	expectedValue.Id = []int32{0, 1, 2}
	expectedValue.Date = []string{"12/7/2016", "12/8/2016", "12/9/2016"}
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
	csvReader := CsvReader{"." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker",
		"." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "event",
		 "{ticker}-{timeframe}.csv", "{eventname}.csv", "1/2/2006"}

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
	csvReader := CsvReader{"." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker",
		"." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "event",
		 "{ticker}-{timeframe}.csv", "{eventname}.csv", "1/2/2006"}
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
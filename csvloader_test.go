package marketdata

import (
    "testing"
    "reflect"
    "errors"
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
        "date": 0,
        "open": 1,
        "high": 2,
        "low": 3,
        "close": 4,
        "volume": 5,
    }
    if ((result["date"] != expectedValue["date"]) ||
       (result["open"] != expectedValue["open"]) ||
       (result["high"] != expectedValue["high"]) ||
       (result["low"] != expectedValue["low"]) || 
       (result["close"] != expectedValue["close"]) ||
       (result["volume"] != expectedValue["volume"])) {
        t.Log("Failed to get column positions. Result was: ", result, " but should be:", expectedValue)
        t.Fail()
    }
}

func TestLoadEventData(t *testing.T) {
    csvLoader := CsvLoader{".\\testdata", "{ticker}-{timeframe}.csv", "{eventname}.csv"}
    var event Event
    event.Name = "testevent"

    result, _ := csvLoader.LoadEventData(&event)
    expectedValue := map[string]bool{
        "5/26/2000": true,
        "7/11/2000": true,
        "9/6/2011": true,
    }
     if ((result.Date["5/26/2000"] != expectedValue["5/26/2000"]) ||
       (result.Date["7/11/2000"] != expectedValue["7/11/2000"]) ||
       (result.Date["9/6/2011"] != expectedValue["9/6/2011"])) {
        t.Log("Failed load EventData. Result was: ", result," but should be: ", expectedValue)
        t.Fail()
    }
}

func TestLoadTickerData(t *testing.T) {
    csvLoader := CsvLoader{".\\testdata", "{ticker}-{timeframe}.csv", "{eventname}.csv"}
    var ticker Ticker
    ticker.Symbol = "someticker"
    ticker.TimeFrame = make([]string, 1)
    ticker.TimeFrame[0] = "daily"

    result, _ := csvLoader.LoadTickerData(&ticker)
    var expectedValue TickerData
    expectedValue.Date = []string{"12/7/2016", "12/8/2016", "12/9/2016"}
    expectedValue.Open = []float64{134.58, 136.25, 138.39}
    expectedValue.High = []float64{136.17, 138.21, 138.82}
    expectedValue.Low = []float64{134.17, 135.80, 137.75}
    expectedValue.Close = []float64{135.89, 138.03, 138.30}
    expectedValue.Volume = []int64{30859300, 47794400, 34276600}

    if (!reflect.DeepEqual(result, expectedValue)) {
        t.Log("Failed load TickerData. Result was: ", result," but should be: ", expectedValue)
        t.Fail()
    }
     
}

func TestLoadTickerDataHandlesErrors(t *testing.T) {
    testCases := []struct{
        name  string
        symbol string
        timeFrame []string
        errorMsg string
    }{
        {"fileDoesNotExist", "invalidTicker", []string{"daily"}, "open .\\testdata\\ticker\\invalidTicker-daily.csv: The system cannot find the file specified."},
        {"tickerFileNoHeader", "noheader", []string{"daily"}, "Invalid CSV Header. Should contain: date,open,high,low,close,volume"},
    }
    csvLoader := CsvLoader{".\\testdata", "{ticker}-{timeframe}.csv", "{eventname}.csv"}
    var ticker Ticker

     for _, tc := range testCases {
        ticker.Symbol = tc.symbol
        ticker.TimeFrame = tc.timeFrame
        _, err := csvLoader.LoadTickerData(&ticker)
        var expectedError = errors.New(tc.errorMsg)
       
        if (err == nil || err.Error() != expectedError.Error()) {
          t.Log("LoadTickerData test case ",tc.name, " did not handle invalid ticker file. Error was: ", err," but should be: ", expectedError)
          t.Fail()
        }
     }
    
}
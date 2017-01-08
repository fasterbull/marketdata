package marketdata

import "testing"

func TestWriteTickerData(t *testing.T) {
	csvWriter := CsvWriter{".\\testdata\\ticker\\processed\\", "{ticker}-{timeframe}.csv", "2006-01-02"}
	symbol := "somedata"
	baseTimeFrame := "daily"
	tickerConfig := WriteConfig{"daily", false}
	addFields := []string{"weekly_id", "monthly_id", "id"}
	higherTfs := []string{"weekly", "monthly"}
	dateFormat := "2006-01-02"
	inputTickerData, _ := getTestTickerData("desc", 0)
	processedTd := processRawTickerData(&inputTickerData, baseTimeFrame, addFields, higherTfs, dateFormat)
	var err error
	err = csvWriter.WriteTickerData(symbol, &processedTd, &tickerConfig)
	if err != nil {
		t.Log("Error was ", err)
		t.Fail()
	}
}

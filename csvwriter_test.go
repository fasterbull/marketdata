package marketdata

import "testing"

func TestWriteTickerData(t *testing.T) {
	csvWriter := CsvWriter{".\\testdata\\ticker\\processed\\", "{ticker}-{timeframe}.csv", "1/2/2006"}
	symbol := "someticker"
	baseTimeFrame := "daily"
	tickerConfig := WriteConfig{"daily", false}
	addFields := []string{"weekly_id", "monthly_id", "id"}
	higherTfs := []string{"weekly", "monthly"}
	dateFormat := "1/2/2006"
	inputTickerData, _ := getTestTickerData("desc", 0)
	processedTd := processRawTickerData(&inputTickerData, baseTimeFrame, addFields, higherTfs, dateFormat)
	err := csvWriter.WriteTickerData(symbol, &processedTd, &tickerConfig)
	if err != nil {
		t.Log("Error was ", err)
		t.Fail()
	}
}

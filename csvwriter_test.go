package marketdata

import "testing"

func TestWriteTickerData(t *testing.T) {
	csvWriter := CsvWriter{".\\testdata\\ticker\\processed\\", "{ticker}-{timeframe}.csv", "1/2/2006"}
	symbol := "someticker"
	tickerConfig := WriteConfig{"daily", false}

	var tickerData TickerData

	tickerData.Date = []string{"12/10/2016", "12/9/2016", "12/8/2016", "12/7/2016"}
	tickerData.Open = []float64{134.58, 136.25, 138.39, 12}
	tickerData.High = []float64{136.17, 138.21, 138.82, 12}
	tickerData.Low = []float64{134.17, 135.80, 137.75, 13}
	tickerData.Close = []float64{135.89, 138.03, 138.30, 13}
	tickerData.Volume = []int64{30859300, 47794400, 34276600, 15}

	err := csvWriter.WriteTickerData(symbol, &tickerData, &tickerConfig)
	if err != nil {
		t.Log("Error was ", err)
		t.Fail()
	}

}

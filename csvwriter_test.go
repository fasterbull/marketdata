package marketdata

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestWriteTickerData(t *testing.T) {
	outputPath := "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker" + string(os.PathSeparator) + "processed" + string(os.PathSeparator)
	outputPathForCompareData := "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker" + string(os.PathSeparator) + "expected" + string(os.PathSeparator)
	csvWriter := CsvWriter{outputPath, "{ticker}-{timeframe}.csv", "1/2/2006"}
	symbol := "testticker"
	baseTimeFrame := "daily"
	tickerConfig := WriteConfig{"daily", false}
	addFields := []string{"weekly_id", "monthly_id", "id"}
	higherTfs := []string{"weekly", "monthly"}
	dateFormat := "1/2/2006"
	inputTickerData, _ := getTestTickerData("asc", 0)
	processedTd := processRawTickerData(&inputTickerData, baseTimeFrame, addFields, higherTfs, dateFormat)
	var err error
	var data []byte
	expectedFile := outputPath + symbol + "-" + baseTimeFrame + ".csv"
	compareFile := outputPathForCompareData + symbol + "-" + baseTimeFrame + "-expected.csv"
	err = csvWriter.WriteTickerData(symbol, &processedTd, &tickerConfig)
	data, err = ioutil.ReadFile(expectedFile)
	if err != nil {
		t.Log("Failed write TickerData. Error is: ", err)
	}
	expectedValue, err := ioutil.ReadFile(compareFile)
	if err != nil {
		t.Log("Failed read expected data from file. Error is: ", err)
	}
	if string(data) != string(expectedValue) {
		t.Log("Failed write TickerData. Result was: ", string(data), " but should be: ", string(expectedValue))
		t.Fail()
	}
	os.Remove(expectedFile)
}

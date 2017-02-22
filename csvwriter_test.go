package marketdata

import (
	"io/ioutil"
	"os"
	"testing"
)

func Test_writeTickerData(t *testing.T) {
	testCases := []struct {
		name       string
		append     bool
		fileExists bool
	}{
		{"'WriteTickerData with WriteConfig.Append set to false and file DOES NOT exist'", false, false},
		{"'WriteTickerData with WriteConfig.Append set to true and file DOES NOT exist'", true, false},
		{"'WriteTickerData with WriteConfig.Append set to true and file DOES exist'", true, true},
	}
	outputPath := "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker" + string(os.PathSeparator) + "processed" + string(os.PathSeparator)
	csvWriter := CsvWriter{outputPath, "{ticker}-{timeframe}.csv", "1/2/2006"}
	symbol := "testticker"
	baseTimeFrame := "daily"
	processedTd := getExpectedDailyDataWithWeeklyAndMonthlyIds()
	var err error
	var result []byte
	resultingFile := outputPath + symbol + "-" + baseTimeFrame + ".csv"
	for _, tc := range testCases {
		tickerConfig := WriteConfig{"daily", tc.append}
		err = csvWriter.writeTickerData(symbol, &processedTd, &tickerConfig)
		result, err = ioutil.ReadFile(resultingFile)
		if err != nil {
			t.Log("Failed write TickerData. Error is: ", err)
		}
		expectedValue := getExpectedCsvDailyDataWithMonthlyWeeklyIds()
		if string(result) != expectedValue {
			t.Log("test_writeTickerData test case ", tc.name, " failed to write tickerData. Result was: ", string(result), " but should be: ", expectedValue)
			t.Fail()
		}
		os.Remove(resultingFile)
	}
}

func getExpectedCsvDailyDataWithMonthlyWeeklyIds() string {
	return "id,monthly_id,weekly_id,date,open,high,low,close,volume\n" +
		"0,-1,-1,11/28/2016,221.16,221.48,220.36,220.48,76572500\n" +
		"1,-1,-1,11/29/2016,220.52,221.44,220.17,220.91,69886700\n" +
		"2,-1,-1,11/30/2016,221.63,221.82,220.31,220.38,113291800\n" +
		"3,0,-1,12/1/2016,220.73,220.73,219.15,219.57,79040500\n" +
		"4,0,-1,12/2/2016,219.67,220.25,219.26,219.68,74840300\n" +
		"5,0,0,12/5/2016,220.65,221.4,220.42,221,67837800\n" +
		"6,0,0,12/6/2016,221.22,221.74,220.66,221.7,59877400\n" +
		"7,0,0,12/7/2016,221.52,224.67,221.38,224.6,110738100\n" +
		"8,0,0,12/8/2016,224.57,225.7,224.26,225.15,99714400\n" +
		"9,0,0,12/9/2016,225.41,226.53,225.37,226.51,88005800\n" +
		"10,0,1,12/12/2016,226.4,226.96,225.76,226.25,102016100\n" +
		"11,0,1,12/13/2016,227.02,228.34,227,227.76,110477500\n" +
		"12,0,1,12/14/2016,227.41,228.23,225.37,225.88,142501800\n" +
		"13,0,1,12/15/2016,226.16,227.81,225.89,226.81,124972600\n" +
		"14,0,1,12/16/2016,226.01,226.08,224.67,225.04,156420200\n" +
		"15,0,2,12/19/2016,225.25,226.02,225.08,225.53,90341100\n" +
		"16,0,2,12/20/2016,226.15,226.57,225.88,226.4,89838800\n" +
		"17,0,2,12/21/2016,226.25,226.45,225.77,225.77,67909000\n" +
		"18,0,2,12/22/2016,225.6,225.74,224.92,225.38,56219100\n" +
		"19,0,2,12/23/2016,225.43,225.72,225.21,225.71,36251400\n" +
		"20,0,3,12/27/2016,226.02,226.73,226,226.27,41054400\n" +
		"21,0,3,12/28/2016,226.02,226.73,226,226.27,41054400\n" +
		"22,0,3,12/29/2016,226.02,226.73,226,226.27,41054400\n" +
		"23,0,3,12/30/2016,226.02,226.73,226,226.27,41054400\n" +
		"24,1,4,1/2/2017,226.02,226.73,226,226.27,41054400\n"
}

func getExpectedCsvWeeklyDataWithMonthlyIds() string {
	return "id,monthly_id,date,open,high,low,close,volume\n" +
		"0,-1,11/28/2016,221.16,221.82,219.15,219.68,413631800\n" +
		"1,0,12/5/2016,220.65,226.53,220.42,226.51,426173500\n" +
		"2,0,12/12/2016,226.4,228.34,224.67,225.04,636388200\n" +
		"3,0,12/19/2016,225.25,226.57,224.92,225.71,340559400\n" +
		"4,0,12/27/2016,226.02,226.73,226,226.27,164217600\n"
}

func getExpectedCsvMonthlyData() string {
	return "id,date,open,high,low,close,volume\n" +
		"0,11/28/2016,221.16,221.82,220.17,220.38,259751000\n" +
		"1,12/1/2016,220.73,228.34,219.15,226.27,1721219500\n"
}

func getExpectedCsvDailyData() string {
	return "id,date,open,high,low,close,volume\n" +
		"0,11/28/2016,221.16,221.48,220.36,220.48,76572500\n" +
		"1,11/29/2016,220.52,221.44,220.17,220.91,69886700\n" +
		"2,11/30/2016,221.63,221.82,220.31,220.38,113291800\n" +
		"3,12/1/2016,220.73,220.73,219.15,219.57,79040500\n" +
		"4,12/2/2016,219.67,220.25,219.26,219.68,74840300\n" +
		"5,12/5/2016,220.65,221.4,220.42,221,67837800\n" +
		"6,12/6/2016,221.22,221.74,220.66,221.7,59877400\n" +
		"7,12/7/2016,221.52,224.67,221.38,224.6,110738100\n" +
		"8,12/8/2016,224.57,225.7,224.26,225.15,99714400\n" +
		"9,12/9/2016,225.41,226.53,225.37,226.51,88005800\n" +
		"10,12/12/2016,226.4,226.96,225.76,226.25,102016100\n" +
		"11,12/13/2016,227.02,228.34,227,227.76,110477500\n" +
		"12,12/14/2016,227.41,228.23,225.37,225.88,142501800\n" +
		"13,12/15/2016,226.16,227.81,225.89,226.81,124972600\n" +
		"14,12/16/2016,226.01,226.08,224.67,225.04,156420200\n" +
		"15,12/19/2016,225.25,226.02,225.08,225.53,90341100\n" +
		"16,12/20/2016,226.15,226.57,225.88,226.4,89838800\n" +
		"17,12/21/2016,226.25,226.45,225.77,225.77,67909000\n" +
		"18,12/22/2016,225.6,225.74,224.92,225.38,56219100\n" +
		"19,12/23/2016,225.43,225.72,225.21,225.71,36251400\n" +
		"20,12/27/2016,226.02,226.73,226,226.27,41054400\n" +
		"21,12/28/2016,226.02,226.73,226,226.27,41054400\n" +
		"22,12/29/2016,226.02,226.73,226,226.27,41054400\n" +
		"23,12/30/2016,226.02,226.73,226,226.27,41054400\n" +
		"24,1/2/2017,226.02,226.73,226,226.27,41054400\n"
}

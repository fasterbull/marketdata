package marketdata

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestWriteTickerData(t *testing.T) {
	dateFormat := "1/2/2006"
	outputPath := "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker" + string(os.PathSeparator) + "processed" + string(os.PathSeparator)
	csvWriter := CsvWriter{outputPath, "{ticker}-{timeframe}.csv", dateFormat}
	tickerForWrite := TickerForWrite{"testticker", "daily", []WriteConfig{{"daily", false}, {"weekly", false}, {"monthly", false}}}
	processedTd := getExpectedDailyDataWithWeeklyAndMonthlyIds()
	var err error
	var result []byte
	err = WriteTickerData(csvWriter, &processedTd, &tickerForWrite)
	if err != nil {
		t.Log("Failed write TickerData. Error is: ", err)
	}

	for _, config := range tickerForWrite.Config {
		resultingFile := outputPath + tickerForWrite.Symbol + "-" + config.TimeFrame + ".csv"
		result, err = ioutil.ReadFile(resultingFile)
		if err != nil {
			t.Log("Failed write TickerData. Error is: ", err)
		}
		expectedValue := getExpectedCsvData(tickerForWrite.BaseTimeFrame, config.TimeFrame)
		if string(result) != expectedValue {
			t.Log("Failed to write TickerData. Result was: ", string(result), " but should be: ", expectedValue)
			t.Fail()
		}
		os.Remove(resultingFile)
	}
}

func TestReadSplitDataAndSort(t *testing.T) {
	var csvReader CsvReader
	csvReader.TickerDataPath = "." + string(os.PathSeparator) + "testdata" + string(os.PathSeparator) + "ticker"
	csvReader.SplitFileNamePattern = "{ticker}-yahoosplitdividend.csv"
	csvReader.DateFormat = "20060102"
	symbol := "someticker"
	result, err := ReadSplitData(csvReader, symbol, "yahoo")
	var expectedValue TickerSplitData
	dates := []string{"20020605", "20050609"}
	expectedValue.Date = createDates(dates, csvReader.DateFormat)
	expectedValue.BeforeSplitQty = []int{2, 1}
	expectedValue.AfterSplitQty = []int{3, 2}
	if !reflect.DeepEqual(result, expectedValue) || err != nil {
		t.Log("Failed ReadTickerSplitData. Result was: ", result, " but should be: ", expectedValue)
		t.Log("Returned error is:", err)
		t.Fail()
	}
}

func getExpectedCsvData(baseTimeFrame string, targetTimeFrame string) string {
	if baseTimeFrame == "daily" {
		if targetTimeFrame == "weekly" {
			return getExpectedCsvWeeklyDataWithMonthlyIds()
		} else if targetTimeFrame == "monthly" {
			return getExpectedCsvMonthlyData()
		} else {
			return getExpectedCsvDailyDataWithMonthlyWeeklyIds()
		}
	}
	return ""
}

func TestProcessRawTickerData(t *testing.T) {
	testCases := []struct {
		name           string
		higherTfs      []string
		addFields      []string
		inputSortOrder string
	}{
		{"'Add weekly ids'", []string{"weekly"}, []string{"weekly_id", "id"}, "desc"},
		{"'Add monthly ids'", []string{"monthly"}, []string{"monthly_id", "id"}, "asc"},
		{"'Add weekly and monthly ids'", []string{"weekly", "monthly"}, []string{"weekly_id", "monthly_id", "id"}, "desc"},
	}
	baseTimeFrame := "daily"
	var tsd TickerSplitData
	for _, tc := range testCases {
		inputTickerData, _ := getTestTickerData(tc.inputSortOrder, 0)
		expectedResult, _ := getTestTickerData("asc", 0)
		expectedResult.HigherTfIds = make(map[string][]int32)
		processedTd := ProcessRawTickerData(&inputTickerData, &tsd, baseTimeFrame, tc.addFields, tc.higherTfs)
		for _, higherTf := range tc.higherTfs {
			if higherTf == "weekly" {
				expectedResult.HigherTfIds["weekly_id"] = []int32{-1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 4}
			} else if higherTf == "monthly" {
				expectedResult.HigherTfIds["monthly_id"] = []int32{-1, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
			}
		}
		expectedResult.Id = []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}
		if !reflect.DeepEqual(processedTd, expectedResult) {
			t.Log("TestAddHigherTimeFrameIds test case ", tc.name, " failed to add HigherTfIds. Result was: ", processedTd, " but should be: ", expectedResult)
			t.Fail()
		}
	}
}

func TestAdjustTickerDataForSplits(t *testing.T) {
	tsd := getTickerSplitData()
	td, err := getTestTickerData("asc", 0)
	if err == nil {
		tdNew := AdjustTickerDataForSplits(&td, &tsd)
		fmt.Printf("Value is %v\n", tdNew)
	}
}

func TestCreateFromLowerTimeFrame(t *testing.T) {
	testCases := []struct {
		name               string
		targetTimeFrame    string
		higherTfs          []string
		addFields          []string
		expectedResultKey  string
		dataSubtractAmount int
	}{
		{"'Add weekly ids from daily data with partial week'", "weekly", []string{"weekly"}, []string{"weekly_id", "id"}, "weekly", 0},
		{"'Add monthly ids from daily data with partial month'", "monthly", []string{"monthly"}, []string{"monthly_id", "id"}, "monthly", 0},
		{"'Add weekly and monthly ids with partial week and month'", "weekly", []string{"weekly", "monthly"}, []string{"weekly_id", "monthly_id", "id"}, "weeklywithmonthly", 0},
		{"'Add weekly ids from daily data with completed week'", "weekly", []string{"weekly"}, []string{"weekly_id", "id"}, "weekly", 1},
		{"'Add monthly ids from daily data with completed month'", "monthly", []string{"monthly"}, []string{"monthly_id", "id"}, "monthly", 1},
		{"'Add weekly and monthly ids with completed week and month'", "weekly", []string{"weekly", "monthly"}, []string{"weekly_id", "monthly_id", "id"}, "weeklywithmonthly", 1},
	}
	baseTimeFrame := "daily"
	var tsd TickerSplitData
	for _, tc := range testCases {
		inputTickerData, _ := getTestTickerData("asc", tc.dataSubtractAmount)
		processedTd := ProcessRawTickerData(&inputTickerData, &tsd, baseTimeFrame, tc.addFields, tc.higherTfs)
		newTfTickerData, _ := createFromLowerTimeFrame(&processedTd, tc.targetTimeFrame)
		expectedResult, _ := getExpectedHigherTfData(tc.expectedResultKey)
		if !reflect.DeepEqual(newTfTickerData, expectedResult) {
			t.Log("TestCreateFromLowerTimeFrame test case ", tc.name, " failed to create TickerData from a lower time frame. Result was: ", newTfTickerData, " but should be: ", expectedResult)
			t.Fail()
		}
	}
}

func getTestTickerData(order string, dataSubtractAmount int) (TickerData, error) {
	var err error
	if order == "asc" {
		return getAscTestTickerData(dataSubtractAmount), err
	} else if order == "desc" {
		return getDescTestTickerData(dataSubtractAmount), err
	}
	var td TickerData
	return td, errors.New("Order must be 'asc' or 'desc'")
}

func getDescTestTickerData(dataSubtractAmount int) TickerData {
	var td TickerData
	dates := []string{"1/2/2017", "12/30/2016", "12/29/2016", "12/28/2016", "12/27/2016", "12/23/2016", "12/22/2016", "12/21/2016", "12/20/2016",
		"12/19/2016", "12/16/2016", "12/15/2016", "12/14/2016", "12/13/2016", "12/12/2016", "12/9/2016", "12/8/2016",
		"12/7/2016", "12/6/2016", "12/5/2016", "12/2/2016", "12/1/2016", "11/30/2016", "11/29/2016", "11/28/2016"}
	td.Date = createDates(dates, "1/2/2006")
	td.Open = []float64{226.02, 226.02, 226.02, 226.02, 226.02, 225.43, 225.60, 226.25, 226.15, 225.25, 226.01, 226.16,
		227.41, 227.02, 226.40, 225.41, 224.57, 221.52, 221.22, 220.65, 219.67, 220.73, 221.63, 220.52, 221.16}
	td.High = []float64{226.73, 226.73, 226.73, 226.73, 226.73, 225.72, 225.74, 226.45, 226.57, 226.02, 226.08, 227.81, 228.23,
		228.34, 226.96, 226.53, 225.70, 224.67, 221.74, 221.40, 220.25, 220.73, 221.82, 221.44, 221.48}
	td.Low = []float64{226.00, 226.00, 226.00, 226.00, 226.00, 225.21, 224.92, 225.77, 225.88, 225.08, 224.67, 225.89, 225.37, 227.00,
		225.76, 225.37, 224.26, 221.38, 220.66, 220.42, 219.26, 219.15, 220.31, 220.17, 220.36}
	td.Close = []float64{226.27, 226.27, 226.27, 226.27, 226.27, 225.71, 225.38, 225.77, 226.40, 225.53, 225.04, 226.81, 225.88,
		227.76, 226.25, 226.51, 225.15, 224.60, 221.70, 221.00, 219.68, 219.57, 220.38, 220.91, 220.48}
	td.Volume = []int64{41054400, 41054400, 41054400, 41054400, 41054400, 36251400, 56219100, 67909000, 89838800, 90341100, 156420200,
		124972600, 142501800, 110477500, 102016100, 88005800, 99714400, 110738100, 59877400, 67837800, 74840300, 79040500, 113291800, 69886700, 76572500}
	if dataSubtractAmount == 0 {
		return td
	}
	return getTickerDataSlice(&td, dataSubtractAmount)
}

func getAscTestTickerData(dataSubtractAmount int) TickerData {
	var td TickerData
	dates := []string{"11/28/2016", "11/29/2016", "11/30/2016", "12/1/2016", "12/2/2016", "12/5/2016", "12/6/2016", "12/7/2016", "12/8/2016", "12/9/2016",
		"12/12/2016", "12/13/2016", "12/14/2016", "12/15/2016", "12/16/2016", "12/19/2016", "12/20/2016", "12/21/2016", "12/22/2016", "12/23/2016", "12/27/2016",
		"12/28/2016", "12/29/2016", "12/30/2016", "1/2/2017"}
	td.Date = createDates(dates, "1/2/2006")
	td.Open = []float64{221.16, 220.52, 221.63, 220.73, 219.67, 220.65, 221.22, 221.52, 224.57, 225.41, 226.40, 227.02, 227.41, 226.16, 226.01,
		225.25, 226.15, 226.25, 225.60, 225.43, 226.02, 226.02, 226.02, 226.02, 226.02}
	td.High = []float64{221.48, 221.44, 221.82, 220.73, 220.25, 221.40, 221.74, 224.67, 225.70, 226.53, 226.96, 228.34, 228.23, 227.81,
		226.08, 226.02, 226.57, 226.45, 225.74, 225.72, 226.73, 226.73, 226.73, 226.73, 226.73}
	td.Low = []float64{220.36, 220.17, 220.31, 219.15, 219.26, 220.42, 220.66, 221.38, 224.26, 225.37, 225.76, 227.00, 225.37, 225.89, 224.67,
		225.08, 225.88, 225.77, 224.92, 225.21, 226.00, 226.00, 226.00, 226.00, 226.00}
	td.Close = []float64{220.48, 220.91, 220.38, 219.57, 219.68, 221.00, 221.70, 224.60, 225.15, 226.51, 226.25, 227.76, 225.88, 226.81,
		225.04, 225.53, 226.40, 225.77, 225.38, 225.71, 226.27, 226.27, 226.27, 226.27, 226.27}
	td.Volume = []int64{76572500, 69886700, 113291800, 79040500, 74840300, 67837800, 59877400, 110738100, 99714400, 88005800, 102016100,
		110477500, 142501800, 124972600, 156420200, 90341100, 89838800, 67909000, 56219100, 36251400, 41054400, 41054400, 41054400, 41054400, 41054400}
	if dataSubtractAmount == 0 {
		return td
	}
	return getTickerDataSlice(&td, dataSubtractAmount)
}

func getTickerDataSlice(td *TickerData, dataSubtractAmount int) TickerData {
	var tdSlice TickerData
	l := len(td.Date)
	sL := l - dataSubtractAmount
	tdSlice.Date = td.Date[0:sL]
	tdSlice.Open = td.Open[0:sL]
	tdSlice.High = td.High[0:sL]
	tdSlice.Low = td.Low[0:sL]
	tdSlice.Close = td.Close[0:sL]
	tdSlice.Volume = td.Volume[0:sL]
	return tdSlice
}

func getTickerSplitData() TickerSplitData {
	var tsd TickerSplitData
	dates := []string{"12/2/2016", "12/19/2016"}
	tsd.Date = createDates(dates, "1/2/2006")
	tsd.BeforeSplitQty = []int{2, 1}
	tsd.AfterSplitQty = []int{3, 2}
	return tsd
}

func getExpectedHigherTfData(higherTf string) (TickerData, error) {
	var err error
	if higherTf == "weekly" {
		return getExpectedWeeklyData(), err
	} else if higherTf == "monthly" {
		return getExpectedMonthlyData(), err
	} else if higherTf == "weeklywithmonthly" {
		return getExpectedWeeklyDataWithMonthlyIds(), err
	}
	var td TickerData
	return td, errors.New("higherTf must be 'weekly' or 'monthly'")
}

func getExpectedWeeklyDataWithMonthlyIds() TickerData {
	var tickerData TickerData
	tickerData.Id = []int32{0, 1, 2, 3, 4}
	dates := []string{"11/28/2016", "12/5/2016", "12/12/2016", "12/19/2016", "12/27/2016"}
	tickerData.Date = createDates(dates, "1/2/2006")
	tickerData.Open = []float64{221.16, 220.65, 226.4, 225.25, 226.02}
	tickerData.High = []float64{221.82, 226.53, 228.34, 226.57, 226.73}
	tickerData.Low = []float64{219.15, 220.42, 224.67, 224.92, 226}
	tickerData.Close = []float64{219.68, 226.51, 225.04, 225.71, 226.27}
	tickerData.Volume = []int64{413631800, 426173500, 636388200, 340559400, 164217600}
	tickerData.HigherTfIds = make(map[string][]int32)
	tickerData.HigherTfIds["monthly_id"] = []int32{-1, 0, 0, 0, 0}
	return tickerData
}

func getExpectedWeeklyData() TickerData {
	var tickerData TickerData
	tickerData.Id = []int32{0, 1, 2, 3, 4}
	dates := []string{"11/28/2016", "12/5/2016", "12/12/2016", "12/19/2016", "12/27/2016"}
	tickerData.Date = createDates(dates, "1/2/2006")
	tickerData.Open = []float64{221.16, 220.65, 226.4, 225.25, 226.02}
	tickerData.High = []float64{221.82, 226.53, 228.34, 226.57, 226.73}
	tickerData.Low = []float64{219.15, 220.42, 224.67, 224.92, 226}
	tickerData.Close = []float64{219.68, 226.51, 225.04, 225.71, 226.27}
	tickerData.Volume = []int64{413631800, 426173500, 636388200, 340559400, 164217600}
	return tickerData
}

func getExpectedMonthlyData() TickerData {
	var tickerData TickerData
	tickerData.Id = []int32{0, 1}
	dates := []string{"11/28/2016", "12/1/2016"}
	tickerData.Date = createDates(dates, "1/2/2006")
	tickerData.Open = []float64{221.16, 220.73}
	tickerData.High = []float64{221.82, 228.34}
	tickerData.Low = []float64{220.17, 219.15}
	tickerData.Close = []float64{220.38, 226.27}
	tickerData.Volume = []int64{259751000, 1721219500}
	return tickerData
}

func getExpectedDailyData() TickerData {
	var tickerData TickerData
	tickerData.Id = []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}
	dates := []string{"11/28/2016", "11/29/2016", "11/30/2016", "12/1/2016", "12/2/2016", "12/5/2016", "12/6/2016", "12/7/2016", "12/8/2016", "12/9/2016", "12/12/2016", "12/13/2016", "12/14/2016", "12/15/2016", "12/16/2016", "12/19/2016", "12/20/2016", "12/21/2016", "12/22/2016", "12/23/2016", "12/27/2016", "12/28/2016", "12/29/2016", "12/30/2016", "1/2/2017"}
	tickerData.Date = createDates(dates, "1/2/2006")
	tickerData.Open = []float64{221.16, 220.52, 221.63, 220.73, 219.67, 220.65, 221.22, 221.52, 224.57, 225.41, 226.4, 227.02, 227.41, 226.16, 226.01, 225.25, 226.15, 226.25, 225.6, 225.43, 226.02, 226.02, 226.02, 226.02, 226.02}
	tickerData.High = []float64{221.48, 221.44, 221.82, 220.73, 220.25, 221.4, 221.74, 224.67, 225.7, 226.53, 226.96, 228.34, 228.23, 227.81, 226.08, 226.02, 226.57, 226.45, 225.74, 225.72, 226.73, 226.73, 226.73, 226.73, 226.73}
	tickerData.Low = []float64{220.36, 220.17, 220.31, 219.15, 219.26, 220.42, 220.66, 221.38, 224.26, 225.37, 225.76, 227, 225.37, 225.89, 224.67, 225.08, 225.88, 225.77, 224.92, 225.21, 226, 226, 226, 226, 226}
	tickerData.Close = []float64{220.48, 220.91, 220.38, 219.57, 219.68, 221, 221.7, 224.6, 225.15, 226.51, 226.25, 227.76, 225.88, 226.81, 225.04, 225.53, 226.4, 225.77, 225.38, 225.71, 226.27, 226.27, 226.27, 226.27, 226.27}
	tickerData.Volume = []int64{76572500, 69886700, 113291800, 79040500, 74840300, 67837800, 59877400, 110738100, 99714400, 88005800, 102016100, 110477500, 142501800, 124972600, 156420200, 90341100, 89838800, 67909000, 56219100, 36251400, 41054400, 41054400, 41054400, 41054400, 41054400}
	return tickerData
}

func getExpectedDailyDataWithWeeklyAndMonthlyIds() TickerData {
	var tickerData TickerData
	tickerData.Id = []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}
	dates := []string{"11/28/2016", "11/29/2016", "11/30/2016", "12/1/2016", "12/2/2016", "12/5/2016", "12/6/2016", "12/7/2016", "12/8/2016", "12/9/2016", "12/12/2016", "12/13/2016", "12/14/2016", "12/15/2016", "12/16/2016", "12/19/2016", "12/20/2016", "12/21/2016", "12/22/2016", "12/23/2016", "12/27/2016", "12/28/2016", "12/29/2016", "12/30/2016", "1/2/2017"}
	tickerData.Date = createDates(dates, "1/2/2006")
	tickerData.Open = []float64{221.16, 220.52, 221.63, 220.73, 219.67, 220.65, 221.22, 221.52, 224.57, 225.41, 226.4, 227.02, 227.41, 226.16, 226.01, 225.25, 226.15, 226.25, 225.6, 225.43, 226.02, 226.02, 226.02, 226.02, 226.02}
	tickerData.High = []float64{221.48, 221.44, 221.82, 220.73, 220.25, 221.4, 221.74, 224.67, 225.7, 226.53, 226.96, 228.34, 228.23, 227.81, 226.08, 226.02, 226.57, 226.45, 225.74, 225.72, 226.73, 226.73, 226.73, 226.73, 226.73}
	tickerData.Low = []float64{220.36, 220.17, 220.31, 219.15, 219.26, 220.42, 220.66, 221.38, 224.26, 225.37, 225.76, 227, 225.37, 225.89, 224.67, 225.08, 225.88, 225.77, 224.92, 225.21, 226, 226, 226, 226, 226}
	tickerData.Close = []float64{220.48, 220.91, 220.38, 219.57, 219.68, 221, 221.7, 224.6, 225.15, 226.51, 226.25, 227.76, 225.88, 226.81, 225.04, 225.53, 226.4, 225.77, 225.38, 225.71, 226.27, 226.27, 226.27, 226.27, 226.27}
	tickerData.Volume = []int64{76572500, 69886700, 113291800, 79040500, 74840300, 67837800, 59877400, 110738100, 99714400, 88005800, 102016100, 110477500, 142501800, 124972600, 156420200, 90341100, 89838800, 67909000, 56219100, 36251400, 41054400, 41054400, 41054400, 41054400, 41054400}
	tickerData.HigherTfIds = make(map[string][]int32)
	tickerData.HigherTfIds["monthly_id"] = []int32{-1, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	tickerData.HigherTfIds["weekly_id"] = []int32{-1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 4}
	return tickerData
}

func createDates(dates []string, dateFormat string) []time.Time {
	size := len(dates)
	realDates := make([]time.Time, size)
	for x := 0; x < size; x++ {
		realDates[x], _ = time.Parse(dateFormat, dates[x])
	}
	return realDates
}

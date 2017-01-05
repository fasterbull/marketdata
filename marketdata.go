package marketdata

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

type DataReader interface {
	ReadTickerData(symbol string, tickerConfig *ReadConfig) (TickerData, error)
	ReadEventData(event *Event) (EventData, error)
}

type DataWriter interface {
	WriteTickerData(symbol string, tickerConfig *ReadConfig) error
}

type Event struct {
	Name string
}

type DateRange struct {
	StartDate string
	EndDate   string
}

type ReadConfig struct {
	TimeFrame string
	Filter    []string
	Range     DateRange
}

type TickerForRead struct {
	Symbol string
	Config []ReadConfig
}

type WriteConfig struct {
	TimeFrame string
	Append    bool
}

type TickerForWrite struct {
	Symbol        string
	BaseTimeFrame string
	Config        []WriteConfig
}

type TickerData struct {
	Id          []int32
	Date        []string
	Open        []float64
	High        []float64
	Close       []float64
	Low         []float64
	Volume      []int64
	HigherTfIds map[string][]int32
}

type EventData struct {
	Date map[string]bool
}

func ReadTickerData(dataReader DataReader, ticker *TickerForRead) (map[string]TickerData, error) {
	data := make(map[string]TickerData)
	var err error
	for _, config := range ticker.Config {
		data[config.TimeFrame], err = dataReader.ReadTickerData(ticker.Symbol, &config)
		if err != nil {
			break
		}
	}
	return data, err
}

func WriteTickerData(dataWriter DataWriter, ticker *TickerForWrite) error {
	var err error
	return err
}

func ReadEventData(dataReader DataReader, event *Event) (EventData, error) {
	eventData, err := dataReader.ReadEventData(event)
	return eventData, err
}

func (td *TickerData) initialize(header map[string]int, size int) {
	for key := range header {
		if key == "id" {
			td.Id = make([]int32, size)
		} else if key == "date" {
			td.Date = make([]string, size)
		} else if key == "open" {
			td.Open = make([]float64, size)
		} else if key == "high" {
			td.High = make([]float64, size)
		} else if key == "low" {
			td.Low = make([]float64, size)
		} else if key == "close" {
			td.Close = make([]float64, size)
		} else if key == "volume" {
			td.Volume = make([]int64, size)
		} else if strings.Contains(key, "_id") {
			if td.HigherTfIds == nil {
				td.HigherTfIds = make(map[string][]int32)
			}
			td.HigherTfIds[key] = make([]int32, size)
		}
	}
}

func getFields(td *TickerData, additionalFields []string, targetTimeFrame string) map[string]int {
	linkedHtfs := getLinkedHigherTimeFrames(targetTimeFrame)
	field := make(map[string]int)
	i := 0
	if td.Id != nil {
		field["id"] = i
		i++
	}
	if td.Date != nil {
		field["date"] = i
		i++
	}
	if td.Open != nil {
		field["open"] = i
		i++
	}
	if td.High != nil {
		field["high"] = i
		i++
	}
	if td.Low != nil {
		field["low"] = i
		i++
	}
	if td.Close != nil {
		field["close"] = i
		i++
	}
	if td.Volume != nil {
		field["volume"] = i
		i++
	}
	if td.HigherTfIds != nil {
		for key := range td.HigherTfIds {
			if targetTimeFrame == "" || subStringInArray(key, linkedHtfs) {
				field[key] = i
				i++
			}
		}
	}
	for _, f := range additionalFields {
		field[f] = i
		i++
	}
	return field
}

func (td *TickerData) addFromRecords(data []string, fieldIndex map[string]int, index int) error {
	var err error
	var int64 int64
	for key, value := range fieldIndex {
		if key == "id" {
			int64, err = strconv.ParseInt(data[value], 10, 32)
			if err != nil {
				return err
			}
			td.Id[index] = int32(int64)
		} else if key == "date" {
			td.Date[index] = data[value]
		} else if key == "open" {
			td.Open[index], err = strconv.ParseFloat(data[value], 64)
			if err != nil {
				return err
			}
		} else if key == "high" {
			td.High[index], err = strconv.ParseFloat(data[value], 64)
			if err != nil {
				return err
			}
		} else if key == "low" {
			td.Low[index], err = strconv.ParseFloat(data[value], 64)
			if err != nil {
				return err
			}
		} else if key == "close" {
			td.Close[index], err = strconv.ParseFloat(data[value], 64)
			if err != nil {
				return err
			}
		} else if key == "volume" {
			td.Volume[index], err = strconv.ParseInt(data[value], 10, 64)
			if err != nil {
				return err
			}
		} else if strings.Contains(key, "_id") {
			int64, err = strconv.ParseInt(data[value], 10, 32)
			if err != nil {
				return err
			}
			td.HigherTfIds[key][index] = int32(int64)
		}
	}
	return err
}

func processRawTickerData(inTd *TickerData, baseTimeFrame string, additionalFields []string, higherTfs []string, dateFormat string) TickerData {
	fields := getFields(inTd, additionalFields, "")
	td := addFieldsAndSortTickerData(inTd, fields, dateFormat)
	for _, higherTf := range higherTfs {
		td.addHigherTimeFrameIds(baseTimeFrame, higherTf, dateFormat)
	}
	return td
}

func (td *TickerData) createFromLowerTimeFrame(inTd *TickerData, requestedTimeFrame string) error {
	var err error
	l := len(inTd.Id)
	rtfIdField := requestedTimeFrame + "_id"
	rTfIds, ok := inTd.HigherTfIds[rtfIdField]
	if !ok {
		return errors.New("Fields " + requestedTimeFrame + " does not exist in ticker data.")
	}
	fields := getFields(inTd, []string{}, requestedTimeFrame)
	//Account for the Ids starting at -1
	rTfLength := rTfIds[l-1] + 1
	td.initialize(fields, int(rTfLength))
	var i int
	rTfIndex := int32(0)
	prevIdIndex := 0
	date := inTd.Date[0]
	open := inTd.Open[0]
	high := inTd.High[0]
	low := inTd.Low[0]
	volume := inTd.Volume[0]
	for i = 1; i < l; i++ {
		if inTd.HigherTfIds[rtfIdField][i] > inTd.HigherTfIds[rtfIdField][prevIdIndex] {
			td.Date[rTfIndex] = date
			td.Open[rTfIndex] = open
			td.High[rTfIndex] = high
			td.Low[rTfIndex] = low
			td.Close[rTfIndex] = inTd.Close[i-1]
			td.Volume[rTfIndex] = volume
			td.Id[rTfIndex] = inTd.HigherTfIds[rtfIdField][prevIdIndex] + 1
			for key := range td.HigherTfIds {
				td.HigherTfIds[key][rTfIndex] = inTd.HigherTfIds[key][prevIdIndex]
			}
			prevIdIndex = i
			date = inTd.Date[i]
			open = inTd.Open[i]
			high = inTd.High[i]
			low = inTd.Low[i]
			volume = inTd.Volume[i]
			rTfIndex++
			if rTfIndex == rTfLength {
				break
			}
		} else {
			if inTd.High[i] > high {
				high = inTd.High[i]
			}
			if inTd.Low[i] < low {
				low = inTd.Low[i]
			}
			volume = volume + inTd.Volume[i]
		}
	}
	return err
}

func addFieldsAndSortTickerData(inTd *TickerData, fields map[string]int, dateFormat string) TickerData {
	dataInDescOrder := inTd.tickerDataInDescOrder(dateFormat)
	if dataInDescOrder {
		return createTickerDataFromDescOrder(inTd, fields)
	} else {
		return createTickerDataFromAscOrder(inTd, fields)
	}
}

func  createTickerDataFromAscOrder(inTd *TickerData, fields map[string]int) TickerData {
	var td TickerData
	td.initialize(fields, len(inTd.Date))
	l := len(inTd.Date)
	var i int
	for i = 0; i < l; i++ {
		td.addItem(inTd, i, i, i)
	}
	return td
}

func createTickerDataFromDescOrder(inTd *TickerData, fields map[string]int) TickerData {
	var td TickerData
	td.initialize(fields, len(inTd.Date))
	l := len(inTd.Date)
	var i int
	id := -1
	for i = l - 1; i > -1; i-- {
		id++
		td.addItem(inTd, id, i, id)
	}
	return td
}

func (td *TickerData) tickerDataInDescOrder(dateFormat string) bool {
	if len(td.Date) <= 1 {
		return true
	}
	date1, _ := time.Parse(dateFormat, td.Date[0])
	date2, _ := time.Parse(dateFormat, td.Date[1])
	return date1.After(date2)
}

func (td *TickerData) addItem(inTd *TickerData, id int, inIndex int, index int) {
	td.Id[index] = int32(id)
	td.Date[index] = inTd.Date[inIndex]
	td.Open[index] = inTd.Open[inIndex]
	td.High[index] = inTd.High[inIndex]
	td.Low[index] = inTd.Low[inIndex]
	td.Close[index] = inTd.Close[inIndex]
	td.Volume[index] = inTd.Volume[inIndex]
}

func (td *TickerData) addHigherTimeFrameIds(tdTf string, higherTf string, dateFormat string) {
	if tdTf == "daily" {
		if higherTf == "weekly" {
			td.addWeeklyIdToDailyData(dateFormat)
		} else if higherTf == "monthly" {
			td.addMonthlyIdToDailyData(dateFormat)
		}
	}
}

func (td *TickerData) addWeeklyIdToDailyData(dateFormat string) {
	_, ok := td.HigherTfIds["weekly_id"]
	if !ok {
		return
	}
	l := len(td.Date)
	z := getIndexOfStartOfSecondWeek(td.Date, dateFormat)
	if z == -1 {
		return
	}
	var i int
	for i = z - 1; i > -1; i-- {
		td.HigherTfIds["weekly_id"][i] = -1
	}
	weeklyId := int32(0)
	td.HigherTfIds["weekly_id"][z] = weeklyId
	for i = z + 1; i < l; i++ {
		curDate, _ := time.Parse(dateFormat, td.Date[i])
		prevDate, _ := time.Parse(dateFormat, td.Date[i-1])
		if prevDate.Weekday() > curDate.Weekday() {
			weeklyId++
		}
		td.HigherTfIds["weekly_id"][i] = weeklyId
	}
}

func (td *TickerData) addMonthlyIdToDailyData(dateFormat string) {
	_, ok := td.HigherTfIds["monthly_id"]
	if !ok {
		return
	}
	l := len(td.Date)
	z := getIndexOfStartOfSecondMonth(td.Date, dateFormat)
	if z == -1 {
		return
	}
	var i int
	for i = z - 1; i > -1; i-- {
		td.HigherTfIds["monthly_id"][i] = -1
	}
	monthlyId := int32(0)
	td.HigherTfIds["monthly_id"][z] = monthlyId
	for i = z + 1; i < l; i++ {
		curDate, _ := time.Parse(dateFormat, td.Date[i])
		prevDate, _ := time.Parse(dateFormat, td.Date[i-1])
		if curDate.Month() != prevDate.Month() {
			monthlyId++
		}
		td.HigherTfIds["monthly_id"][i] = monthlyId
	}
}

func getLinkedHigherTimeFrames(targetTimeFrame string) []string {
	switch tf := targetTimeFrame; tf {
	case "daily":
		return []string{"weekly", "monthly"}
	case "weekly":
		return []string{"monthly"}
	default:
		return []string{}
	}
}

func getIndexOfStartOfSecondWeek(date []string, dateFormat string) int {
	l := len(date)
	if l <= 1 {
		return -1
	}
	for i := 1; i < l; i++ {
		curDate, _ := time.Parse(dateFormat, date[i])
		prevDate, _ := time.Parse(dateFormat, date[i-1])
		if prevDate.Weekday() > curDate.Weekday() {
			return i
		}
	}
	return -1
}

func getIndexOfStartOfSecondMonth(date []string, dateFormat string) int {
	l := len(date)
	if l <= 1 {
		return -1
	}
	for i := 1; i < l; i++ {
		curDate, _ := time.Parse(dateFormat, date[i])
		prevDate, _ := time.Parse(dateFormat, date[i-1])
		if curDate.Month() != prevDate.Month() {
			return i
		}
	}
	return -1
}

func inArray(value string, array []string) bool {
	for _, item := range array {
		if strings.ToLower(value) == strings.ToLower(item) {
			return true
		}
	}
	return false
}

func subStringInArray(value string, array []string) bool {
	for _, item := range array {
		if strings.Contains(strings.ToLower(value), strings.ToLower(item)) {
			return true
		}
	}
	return false
}

func getDefaultHeader() map[string]int {
	header := make(map[string]int)
	header["id"] = 0
	header["date"] = 1
	header["open"] = 2
	header["high"] = 3
	header["low"] = 4
	header["close"] = 5
	header["volume"] = 6

	return header
}

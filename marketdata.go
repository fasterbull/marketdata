package marketdata

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
)

type DataSource string

const (
	YAHOO DataSource = "YAHOO"
	OTHER            = "OTHER"
)

type DataReader interface {
	readTickerData(symbol string, tickerConfig *ReadConfig) (TickerData, error)
	readEventData(event *Event) (EventData, error)
	readDividendData(symbol string, source DataSource) (TickerDividendData, error)
	readSplitData(symbol string, source DataSource) (TickerSplitData, error)
	getDateFormat() string
}

type Data interface {
	addFromRecords(data []string, fieldIndex map[string]int, index int, dateFormat string) error
	initialize(size int)
}

type DataWriter interface {
	writeTickerData(symbol string, tickerData *TickerData, tickerConfig *WriteConfig) error
}

type Event struct {
	Name string
}

type DateRange struct {
	StartDate time.Time
	EndDate   time.Time
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
	Date        []time.Time
	Open        []float64
	High        []float64
	Close       []float64
	Low         []float64
	Volume      []int64
	HigherTfIds map[string][]int32
}

type TickerSplitData struct {
	Date           []time.Time
	BeforeSplitQty []int
	AfterSplitQty  []int
}

type TickerDividendData struct {
	Date   []time.Time
	Amount []float64
}

type EventData struct {
	Date map[time.Time]bool
}

func ReadTickerData(dataReader DataReader, ticker *TickerForRead) (map[string]*TickerData, error) {
	data := make(map[string]*TickerData)
	var err error
	for _, config := range ticker.Config {
		td, err := dataReader.readTickerData(ticker.Symbol, &config)
		if err != nil {
			break
		}
		data[config.TimeFrame] = &td
	}
	return data, err
}

func ReadEventData(dataReader DataReader, event *Event) (EventData, error) {
	eventData, err := dataReader.readEventData(event)
	return eventData, err
}

func ReadSplitData(dataReader DataReader, symbol string, source DataSource) (TickerSplitData, error) {
	tsd, err := dataReader.readSplitData(symbol, source)
	if dataInDescOrder(tsd.Date) {
		tsd = sortSplitDataInAscOrder(&tsd, createSplitDataHeaderMap())
	}
	return tsd, err
}

func ReadDividendData(dataReader DataReader, symbol string, source DataSource) (TickerDividendData, error) {
	tdd, err := dataReader.readDividendData(symbol, source)
	if dataInDescOrder(tdd.Date) {
		tdd = sortDividendDataInAscOrder(&tdd, createDividendDataHeaderMap())
	}
	return tdd, err
}

func WriteTickerData(dataWriter DataWriter, inTickerData *TickerData, ticker *TickerForWrite) error {
	var err error
	var tdToWrite *TickerData
	for _, config := range ticker.Config {
		if config.TimeFrame == ticker.BaseTimeFrame {
			tdToWrite = inTickerData
		} else {
			higherTfTd, _ := createFromLowerTimeFrame(inTickerData, config.TimeFrame)
			tdToWrite = &higherTfTd
		}
		err = dataWriter.writeTickerData(ticker.Symbol, tdToWrite, &config)
		if err != nil {
			break
		}
	}
	return err
}

func ProcessRawTickerData(inTd *TickerData, tsd *TickerSplitData, baseTimeFrame string, additionalFields []string, higherTfs []string) TickerData {
	td := createSortedTickerData(inTd, additionalFields)
	if tsd.Date != nil {
		td.AdjustTickerDataForSplits(tsd)
	}
	for _, higherTf := range higherTfs {
		td.addHigherTimeFrameIds(baseTimeFrame, higherTf)
	}
	return td
}

func (td *TickerData) AdjustTickerDataForSplits(tsd *TickerSplitData) {
	size := len(tsd.Date)
	for x := 0; x < size; x++ {
		i := getIndexOfDateValue(td.Date, tsd.Date[x])
		if i > -1 {
			td.adjustTickerDataForSplitEvent(i-1, tsd.BeforeSplitQty[x], tsd.AfterSplitQty[x])
		}
	}
}

func (td *TickerData) initialize(header map[string]int, size int) {
	for key := range header {
		if key == "id" {
			td.Id = make([]int32, size)
		} else if key == "date" {
			td.Date = make([]time.Time, size)
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

func (td *TickerData) addFromRecords(data []string, fieldIndex map[string]int, index int, dateFormat string) error {
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
			td.Date[index], _ = time.Parse(dateFormat, data[value])
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

func (td *TickerData) adjustTickerDataForSplitEvent(index int32, beforeSplityQty int, afterSplitQty int) {
	priceRatio := float64(beforeSplityQty) / float64(afterSplitQty)
	volumeRatio := float32(afterSplitQty) / float32(beforeSplityQty)
	dp := 2
	for x := index; x > -1; x-- {
		td.Open[x] = td.Open[x] * priceRatio
		td.High[x] = roundPlus((td.High[x] * priceRatio), dp)
		td.Low[x] = roundPlus((td.Low[x] * priceRatio), dp)
		td.Close[x] = roundPlus((td.Close[x] * priceRatio), dp)
		td.Volume[x] = int64(float32(td.Volume[x]) * volumeRatio)
	}
}

func (td *TickerData) addItem(inTd *TickerData, id int, inIndex int, index int) {
	if td.Id != nil {
		td.Id[index] = int32(id)
	}
	if td.Date != nil {
		td.Date[index] = inTd.Date[inIndex]
	}
	if td.Open != nil {
		td.Open[index] = inTd.Open[inIndex]
	}
	if td.High != nil {
		td.High[index] = inTd.High[inIndex]
	}
	if td.Low != nil {
		td.Low[index] = inTd.Low[inIndex]
	}
	if td.Close != nil {
		td.Close[index] = inTd.Close[inIndex]
	}
	if td.Volume != nil {
		td.Volume[index] = inTd.Volume[inIndex]
	}
}

func (td *TickerData) addItemFromLowerTimeFrame(inTd *TickerData, requestedTfField string, inIndex int32, index int32, date time.Time, open float64, high float64, low float64, close float64, volume int64) {
	td.Id[index] = inTd.HigherTfIds[requestedTfField][inIndex] + 1
	td.Date[index] = date
	td.Open[index] = open
	td.High[index] = high
	td.Low[index] = low
	td.Close[index] = close
	td.Volume[index] = volume
	for key := range td.HigherTfIds {
		td.HigherTfIds[key][index] = inTd.HigherTfIds[key][inIndex]
	}
}

func (td *TickerData) addHigherTimeFrameIds(tdTf string, higherTf string) {
	if tdTf == "daily" {
		if higherTf == "weekly" {
			td.addWeeklyIdToDailyData()
		} else if higherTf == "monthly" {
			td.addMonthlyIdToDailyData()
		}
	}
}

func (td *TickerData) addWeeklyIdToDailyData() {
	_, ok := td.HigherTfIds["weekly_id"]
	if !ok {
		return
	}
	l := len(td.Date)
	z := getIndexOfStartOfSecondWeek(td.Date)
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
		curDate := td.Date[i]
		prevDate := td.Date[i-1]
		if prevDate.Weekday() > curDate.Weekday() {
			weeklyId++
		}
		td.HigherTfIds["weekly_id"][i] = weeklyId
	}
}

func (td *TickerData) addMonthlyIdToDailyData() {
	_, ok := td.HigherTfIds["monthly_id"]
	if !ok {
		return
	}
	l := len(td.Date)
	z := getIndexOfStartOfSecondMonth(td.Date)
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
		curDate := td.Date[i]
		prevDate := td.Date[i-1]
		if curDate.Month() != prevDate.Month() {
			monthlyId++
		}
		td.HigherTfIds["monthly_id"][i] = monthlyId
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

func createFromLowerTimeFrame(inTd *TickerData, requestedTimeFrame string) (TickerData, error) {
	var err error
	var td TickerData
	l := int32(len(inTd.Id))
	rtfIdField := requestedTimeFrame + "_id"
	_, ok := inTd.HigherTfIds[rtfIdField]
	if !ok {
		return td, errors.New("Fields " + requestedTimeFrame + " does not exist in ticker data.")
	}
	fields := getFields(inTd, []string{}, requestedTimeFrame)
	var lastCompletedTfIndex int32
	lastCompletedTfIndex, err = getLastCompletedTimeFrameIndex(inTd, requestedTimeFrame)
	//Account for the Ids starting at -1
	rTfLength := inTd.HigherTfIds[rtfIdField][lastCompletedTfIndex] + 2
	td.initialize(fields, int(rTfLength))
	rTfIndex := int32(0)
	prevIdIndex := int32(0)
	date := inTd.Date[0]
	open := inTd.Open[0]
	high := inTd.High[0]
	low := inTd.Low[0]
	volume := inTd.Volume[0]
	for i := int32(1); i < l; i++ {
		if i == lastCompletedTfIndex {
			if inTd.High[i] > high {
				high = inTd.High[i]
			}
			if inTd.Low[i] < low {
				low = inTd.Low[i]
			}
			volume = volume + inTd.Volume[i]
			td.addItemFromLowerTimeFrame(inTd, rtfIdField, i, rTfIndex, date, open, high, low, inTd.Close[i], volume)
			break
		}
		if inTd.HigherTfIds[rtfIdField][i] > inTd.HigherTfIds[rtfIdField][prevIdIndex] {
			td.addItemFromLowerTimeFrame(inTd, rtfIdField, prevIdIndex, rTfIndex, date, open, high, low, inTd.Close[i-1], volume)
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
	return td, err
}

func getLastCompletedTimeFrameIndex(td *TickerData, timeFrame string) (int32, error) {
	var err error
	var lastTimeFrameId int32
	l := int32(len(td.Id))
	tfIdField := timeFrame + "_id"
	_, ok := td.HigherTfIds[tfIdField]
	if !ok {
		return lastTimeFrameId, errors.New("Field " + timeFrame + " does not exist in ticker data.")
	}
	lastDate := td.Date[l-1]
	if timeFrame == "weekly" {
		if lastDate.Weekday().String() == "Friday" {
			return int32(l - 1), err
		}
	} else if timeFrame == "monthly" {
		if (lastDate.Month() != lastDate.AddDate(0, 0, 1).Month()) ||
			(lastDate.Weekday().String() == "Friday" && lastDate.Month() != lastDate.AddDate(0, 0, 3).Month()) {
			return int32(l - 1), err
		}
	}
	var index int32
	for i := l - 2; i >= 0; i-- {
		if td.HigherTfIds[tfIdField][i] != td.HigherTfIds[tfIdField][i+1] {
			index = i
			break
		}
	}
	return index, err
}

func createSortedTickerData(inTd *TickerData, additionalFields []string) TickerData {
	fields := getFields(inTd, additionalFields, "")
	dataInDescOrder := dataInDescOrder(inTd.Date)
	if dataInDescOrder {
		return createTickerDataFromDescOrder(inTd, fields)
	} else if len(additionalFields) == 0 {
		return *inTd
	}
	return createTickerDataFromAscOrder(inTd, fields)
}

func createTickerDataFromAscOrder(inTd *TickerData, fields map[string]int) TickerData {
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

func (tsd *TickerSplitData) addItem(inTsd *TickerSplitData, id int, inIndex int, index int) {
	if tsd.Date != nil {
		tsd.Date[index] = inTsd.Date[inIndex]
	}
	if tsd.BeforeSplitQty != nil {
		tsd.BeforeSplitQty[index] = inTsd.BeforeSplitQty[inIndex]
	}
	if tsd.AfterSplitQty != nil {
		tsd.AfterSplitQty[index] = inTsd.AfterSplitQty[inIndex]
	}
}

func (tdd *TickerDividendData) initialize(size int) {
	tdd.Date = make([]time.Time, size)
	tdd.Amount = make([]float64, size)
}

func (tdd *TickerDividendData) addFromRecords(data []string, fieldIndex map[string]int, index int, dateFormat string) error {
	var err error
	for key, value := range fieldIndex {
		if key == "date" {
			tdd.Date[index], _ = time.Parse(dateFormat, strings.TrimSpace(data[value]))
		} else if key == "dividend" {
			tdd.Amount[index], err = strconv.ParseFloat(strings.TrimSpace(data[value]), 64)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func createSplitDataHeaderMap() map[string]int {
	header := make(map[string]int)
	header["date"] = 0
	header["split"] = 1
	return header
}

func (tdd *TickerDividendData) addItem(inTdd *TickerDividendData, id int, inIndex int, index int) {
	if tdd.Date != nil {
		tdd.Date[index] = inTdd.Date[inIndex]
	}
	if tdd.Amount != nil {
		tdd.Amount[index] = inTdd.Amount[inIndex]
	}
}

func createDividendDataHeaderMap() map[string]int {
	header := make(map[string]int)
	header["date"] = 0
	header["amount"] = 1
	return header
}

func sortDividendDataInAscOrder(tdd *TickerDividendData, fields map[string]int) TickerDividendData {
	var tddSorted TickerDividendData
	tddSorted.initialize(len(tdd.Date))
	l := len(tdd.Date)
	var i int
	id := -1
	for i = l - 1; i > -1; i-- {
		id++
		tddSorted.addItem(tdd, id, i, id)
	}
	return tddSorted
}

func (tsd *TickerSplitData) initialize(size int) {
	tsd.Date = make([]time.Time, size)
	tsd.BeforeSplitQty = make([]int, size)
	tsd.AfterSplitQty = make([]int, size)
}

func (tsd *TickerSplitData) addFromRecords(data []string, fieldIndex map[string]int, index int, dateFormat string) error {
	var err error
	var int64val int64
	for key, value := range fieldIndex {
		if key == "date" {
			tsd.Date[index], _ = time.Parse(dateFormat, strings.TrimSpace(data[value]))
		} else if key == "split" {
			splitData := strings.Split(data[value], ":")
			int64val, err = strconv.ParseInt(splitData[1], 10, 16)
			if err != nil {
				return err
			}
			tsd.BeforeSplitQty[index] = int(int64val)
			int64val, err = strconv.ParseInt(splitData[0], 10, 16)
			if err != nil {
				return err
			}
			tsd.AfterSplitQty[index] = int(int64val)
		}
	}
	return err
}

func sortSplitDataInAscOrder(tsd *TickerSplitData, fields map[string]int) TickerSplitData {
	var tsdSorted TickerSplitData
	tsdSorted.initialize(len(tsd.Date))
	l := len(tsd.Date)
	var i int
	id := -1
	for i = l - 1; i > -1; i-- {
		id++
		tsdSorted.addItem(tsd, id, i, id)
	}
	return tsdSorted
}

func getIndexOfDateValue(dates []time.Time, date time.Time) int32 {
	for i := range dates {
		if dates[i] == date {
			return int32(i)
		}
	}
	return -1
}

func numDecimalPlaces(v float64) int {
	s := strconv.FormatFloat(v, 'f', -1, 64)
	i := strings.IndexByte(s, '.')
	if i > -1 {
		return len(s) - i - 1
	}
	return 0
}

func roundPlus(f float64, places int) float64 {
	shift := math.Pow(10, float64(places))
	return round(f*shift) / shift
}

func round(f float64) float64 {
	return math.Floor(f + .5)
}

func dataInDescOrder(date []time.Time) bool {
	if len(date) <= 1 {
		return true
	}
	return date[0].After(date[1])
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

func getIndexOfStartOfSecondWeek(date []time.Time) int {
	l := len(date)
	if l <= 1 {
		return -1
	}
	for i := 1; i < l; i++ {
		curDate := date[i]
		prevDate := date[i-1]
		if prevDate.Weekday() > curDate.Weekday() {
			return i
		}
	}
	return -1
}

func getIndexOfStartOfSecondMonth(date []time.Time) int {
	l := len(date)
	if l <= 1 {
		return -1
	}
	for i := 1; i < l; i++ {
		curDate := date[i]
		prevDate := date[i-1]
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

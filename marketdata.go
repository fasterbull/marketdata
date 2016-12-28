package marketdata

import (
	"strconv"
	"strings"
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

func (td *TickerData) add(data []string, header map[string]int, index int) error {
	var err error
	var int64 int64
	for key, value := range header {
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

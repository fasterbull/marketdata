package marketdata

import (
	"strconv"
 )

type DataLoaderInterface interface {
  LoadTickerData(ticker *Ticker) (TickerData, error)
  LoadEventData(event *Event) (EventData, error)
}

type Event struct {
  Name string
}

type Ticker struct {
  Symbol string
  TimeFrame []string
  Fields []string
}

type TickerData struct {
  Id []int32
  Date []string
  Open []float64
  High []float64
  Close []float64
  Low []float64
  Volume []int64
  HigherTFIds map[string][]int32
}

type EventData struct {
  Date map[string]bool
}

func LoadTickerData(dataLoader DataLoaderInterface, ticker *Ticker) (TickerData, error) {
  tickerData, err := dataLoader.LoadTickerData(ticker)
  return tickerData, err
}

func LoadEventData(dataLoader DataLoaderInterface, event *Event) (EventData, error) {
  eventData, err := dataLoader.LoadEventData(event)
  return eventData, err
}

func (td *TickerData) initialize(header map[string]int, size int) {
     
     var exists bool
     _ , exists = header["id"]
     if exists {
       td.Id = make([]int32, size)
     }

     _ , exists = header["date"]
     if exists {
       td.Date = make([]string, size)
     }

     _ , exists = header["open"]
     if exists {
       td.Open = make([]float64, size)
     }

     _ , exists = header["high"]
     if exists {
       td.High = make([]float64, size)
     }

     _ , exists = header["low"]
     if exists {
       td.Low = make([]float64, size)
     }

     _ , exists = header["close"]
     if exists {
       td.Close = make([]float64, size)
     }

      _ , exists = header["volume"]
     if exists {
       td.Volume = make([]int64, size)
     }

  }
  
  func (td *TickerData) add(data []string, header map[string]int, index int) error {
     
     var exists bool
     var value int
     var err error
     var int64 int64

     value, exists = header["id"]
     if exists {
       int64, err = strconv.ParseInt(data[value], 10, 32)
       td.Id[index] = int32(int64)
     }

     value, exists = header["date"]
     if exists {
       td.Date[index] = data[header["date"]]
     }

     value, exists = header["open"]
     if exists {
       td.Open[index], err = strconv.ParseFloat(data[header["open"]], 64)
     }

     value, exists = header["high"]
     if exists {
       td.High[index], err = strconv.ParseFloat(data[header["high"]], 64)
     }

     value, exists = header["low"]
     if exists {
       td.Low[index], err = strconv.ParseFloat(data[header["low"]], 64)
     }

     value, exists = header["close"]
     if exists {
       td.Close[index], err = strconv.ParseFloat(data[header["close"]], 64)
     }

     value, exists = header["volume"]
     if exists {
       td.Volume[index], err = strconv.ParseInt(data[header["volume"]], 10, 64)
     }

     return err

  }

package marketdata

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
}

type TickerData struct {
  Date []string
  Open []float64
  High []float64
  Close []float64
  Low []float64
  Volume []int64
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

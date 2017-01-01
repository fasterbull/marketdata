package marketdata

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

type CsvWriter struct {
	OutputPath            string
	TickerFileNamePattern string
	DateFormat            string
}

func (csvWriter *CsvWriter) WriteTickerData(symbol string, tickerData *TickerData, tickerConfig *WriteConfig) error {
	newLine := "\n"
	fileName := getTickerDataFileName(csvWriter.TickerFileNamePattern, symbol, tickerConfig.TimeFrame)
	filePath := csvWriter.OutputPath + fileName
	var fwr, fr *os.File
	var err error
	var nextId int
	if tickerConfig.Append {
		fr, err = os.Open(filePath)
		if err != nil {
			return errors.New("File Write Error: " + err.Error())
		}
		nextId, err = getNextId(fr)
		fr.Close()
		fwr, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return errors.New("File Write Error: " + err.Error())
		}
	} else {
		fwr, err = os.Create(filePath)
		if err != nil {
			return errors.New("File Write Error: " + err.Error())
		}
		nextId = 0
	}
	defer fwr.Close()
	writer := bufio.NewWriter(fwr)
	if !tickerConfig.Append {
		printHeader(writer, newLine)
	}

	printTickerData(writer, tickerData, nextId, newLine)

	writer.Flush()
	return err
}

func printTickerData(writer *bufio.Writer, tickerData *TickerData, nextId int, newLine string) {
	l := len(tickerData.Date)
	var i int
	fmt.Printf("Out of the Loop")
	for i = nextId; i < l; i++ {
		fmt.Printf("In the Loop %v", i)
		printTickerDataItem(writer, tickerData, i, i, newLine)
	}
}

func printTickerDataItem(writer *bufio.Writer, tickerData *TickerData, id int, index int, newLine string) {
	fmt.Fprintf(writer, "%v,%v,%v,%v,%v,%v,%v%v", id, tickerData.Date[index], tickerData.Open[index], tickerData.High[index], tickerData.Low[index], tickerData.Close[index], tickerData.Volume[index], newLine)
}

func printHeader(writer *bufio.Writer, newLine string) {
	fmt.Fprintf(writer, "id,open,high,low,close,volume%v", newLine)
}

func getNextId(r io.Reader) (int, error) {
	id, err := lineCounter(r)
	return id, err
}

func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

package marketdata

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type CsvWriter struct {
	OutputPath      string
	FileNamePattern string
	DateFormat      string
}

func (csvWriter CsvWriter) writeTickerData(symbol string, tickerData *TickerData, tickerConfig *WriteConfig) error {
	newLine := "\n"
	fileName := getTickerDataFileName(csvWriter.FileNamePattern, symbol, tickerConfig.TimeFrame)
	filePath := csvWriter.OutputPath
	var fwr, fr *os.File
	var err error
	var nextId int
	fileOpenError := false
	newFile := false
	if tickerConfig.Append {
		fr, err = os.Open(filePath + fileName)
		if err != nil {
			fileOpenError = true
		}
	}
	if tickerConfig.Append && !fileOpenError {
		nextId, err = getNextId(fr)
		fr.Close()
		fwr, err = os.OpenFile(filePath+fileName, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return errors.New("File Write Error: " + err.Error())
		}
	} else {
		newFile = true
		os.MkdirAll(filePath, os.ModePerm)
		fwr, err = os.Create(filePath + fileName)
		if err != nil {
			return errors.New("File Write Error: " + err.Error())
		}
		nextId = 0
	}
	defer fwr.Close()
	writer := bufio.NewWriter(fwr)
	sortedHigherTfIds := getSortedHigherTimeFrameIds(tickerData.HigherTfIds)
	if newFile {
		printHeader(writer, tickerData, sortedHigherTfIds, newLine)
	}
	printTickerData(writer, tickerData, sortedHigherTfIds, nextId, newLine, csvWriter.DateFormat)
	writer.Flush()
	return err
}

func printTickerData(writer *bufio.Writer, tickerData *TickerData, sortedHigherTfIds []string, nextId int, newLine string, dateFormat string) {
	l := len(tickerData.Date)
	var i int
	for i = nextId; i < l; i++ {
		printTickerDataItem(writer, tickerData, sortedHigherTfIds, i, newLine, dateFormat)
	}
}

func printTickerDataItem(writer *bufio.Writer, td *TickerData, sortedHigherTfIds []string, index int, newLine string, dateFormat string) {
	record := ""
	if td.Id != nil {
		record = record + fmt.Sprintf("%v", td.Id[index]) + ","
	}
	if td.HigherTfIds != nil {
		for _, value := range sortedHigherTfIds {
			record = record + fmt.Sprintf("%v", td.HigherTfIds[value][index]) + ","
		}
	}
	if td.Date != nil {
		record = record + td.Date[index].Format(dateFormat) + ","
	}
	if td.Open != nil {
		record = record + fmt.Sprintf("%v", td.Open[index]) + ","
	}
	if td.High != nil {
		record = record + fmt.Sprintf("%v", td.High[index]) + ","
	}
	if td.Low != nil {
		record = record + fmt.Sprintf("%v", td.Low[index]) + ","
	}
	if td.Close != nil {
		record = record + fmt.Sprintf("%v", td.Close[index]) + ","
	}
	if td.Volume != nil {
		record = record + fmt.Sprintf("%v", td.Volume[index]) + ","
	}
	fmt.Fprintf(writer, "%v%v", strings.TrimSuffix(record, ","), newLine)
}

func printHeader(writer *bufio.Writer, td *TickerData, sortedHigherTfIds []string, newLine string) {
	header := ""
	if td.Id != nil {
		header = header + "id,"
	}
	if td.HigherTfIds != nil {
		for _, value := range sortedHigherTfIds {
			header = header + value + ","
		}
	}
	if td.Date != nil {
		header = header + "date,"
	}
	if td.Open != nil {
		header = header + "open,"
	}
	if td.High != nil {
		header = header + "high,"
	}
	if td.Low != nil {
		header = header + "low,"
	}
	if td.Close != nil {
		header = header + "close,"
	}
	if td.Volume != nil {
		header = header + "volume,"
	}
	fmt.Fprintf(writer, "%v%v", strings.TrimSuffix(header, ","), newLine)
}

func getSortedHigherTimeFrameIds(higherTfIds map[string][]int32) []string {
	sortedHigherTfIds := make([]string, len(higherTfIds))
	i := 0
	for key := range higherTfIds {
		sortedHigherTfIds[i] = key
		i++
	}
	sort.Strings(sortedHigherTfIds)
	return sortedHigherTfIds
}

func getNextId(r io.Reader) (int, error) {
	id, err := lineCounter(r)
	return id - 1, err
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

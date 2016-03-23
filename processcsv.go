package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"sync"
)

type outRec struct {
	rec       []string
	processed bool
}

func processLines(inpath string, outpath string, errpath string) {
	infile, _ := os.Open(inpath)
	outfile, _ := os.Create(outpath)
	errfile, _ := os.Create(errpath)
	defer outfile.Close()
	defer errfile.Close()

	jobs := make(chan []string)
	results := make(chan outRec)

	wg := new(sync.WaitGroup)
	for w := 1; w <= 3; w++ {
		wg.Add(1)
		go processLine(jobs, results, mapper, wg)
	}

	go reader(infile, jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	writer(outfile, errfile, results)
}

func reader(infile *os.File, jobs chan<- []string) {
	r := csv.NewReader(bufio.NewReader(infile))
	for {
		line, err := r.Read()
		if err == io.EOF {
			break
		}
		jobs <- line
	}
	close(jobs)
}

func writer(outfile, errfile *os.File, results <-chan outRec) {
	w := csv.NewWriter(outfile)
	e := csv.NewWriter(errfile)

	for row := range results {
		line, processed := row.rec, row.processed
		if processed {
			w.Write(line)
		} else {
			e.Write(line)
		}
	}

	w.Flush()
	e.Flush()
}

func processLine(jobs <-chan []string, results chan<- outRec, mapper func([]string) ([]string, error), wg *sync.WaitGroup) {
	defer wg.Done()

	for line := range jobs {
		mapped, err := mapper(line)
		if err != nil {
			results <- outRec{line, false}
		} else {
			results <- outRec{mapped, true}
		}
	}
}

func mapper(record []string) ([]string, error) {
	if record[0] == "athletics" {
		return record[:3], nil
	} else {
		return record, errors.New("")
	}
}

func main() {
	inpath := "./test2.csv"
	outpath := "./test.out.csv"
	errpath := "./test.err.csv"

	processLines(inpath, outpath, errpath)
}

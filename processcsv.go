package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"sync"
)

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
		go processLine(jobs, results, mapper,  wg)
	}

	go func() {
		r := csv.NewReader(bufio.NewReader(infile))
		for {
			line, err := r.Read()
			if err == io.EOF {
				break
			}
			jobs <- line
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

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

type outRec struct {
	rec []string
	processed bool
}

func processLine(jobs <-chan []string, results chan<- outRec, mapper func([]string) outRec, wg *sync.WaitGroup) {
	defer wg.Done()

	for line := range jobs {
		results <- mapper(line)
	}
}

func mapper(records []string) outRec {
	if records[0] == "athletics" {
		return outRec{records[:2], true}
	} else {
		return outRec{records, false}
	}
}

func main() {
	inpath := "./test3.csv"
	outpath := "./test.out.csv"
	errpath := "./test.err.csv"

	processLines(inpath, outpath, errpath)
}
p

/*
processcsv is a csv processing library which maps records
according to a specified output header and mapper.

TO DO:
access columns via fields names instead of indices.
...
*/

package csvutils

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"sync"
)

// mappedRec contains the mapped record and whether
// or not the original record was processed.
type mappedRec struct {
	rec       []string
	processed bool
}


// pull this out this in a Mapping file?
type Mapping struct {
	Inhdr, Outhdr []string
	Mapper map[string]func([]string)(string, error)
}
	

// ProcessLines parses an input csv file on disk according to the given mapper
// and writes the mapped file to outpath, with unprocessed lines written to errpath.
//
// to do: should take reader/writer interfaces instead of file paths.
func ProcessLines(inpath string, outpath string, errpath string, m Mapping) {
	infile, _ := os.Open(inpath)
	outfile, _ := os.Create(outpath)
	errfile, _ := os.Create(errpath)
	defer outfile.Close()
	defer errfile.Close()

	jobs := make(chan []string)
	results := make(chan mappedRec)

	wg := new(sync.WaitGroup)
	for w := 1; w <= 3; w++ {
		wg.Add(1)
		go processLine(jobs, results, m, wg)
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

func writer(outfile, errfile *os.File, results <-chan mappedRec) {
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


func processLine(jobs <-chan []string, results chan<- mappedRec, m Mapping, wg *sync.WaitGroup) {
	defer wg.Done()
	l := len(m.Outhdr)
	out := make([]string, l)

	for line := range jobs {
		var k int
		for _, field := range m.Outhdr {
			f := m.Mapper[field]
			result, err := f(line)
			if err != nil {
				results <- mappedRec{line, false}
				break
			} else {
				out[k] = result
			}
			k++
		}
		if k == l {
			results <- mappedRec{out, true}
		}
	}
}


package main

import (
	"csvutils"
	"errors"
	"strconv"
)

// udfs for csv cleaning


func isOakland(rec []string)(string, error) {
	if rec[0] == "athletics" {
		return "athletics", nil
	} else {
		return "", errors.New("")
	}
}

func winDiff(rec []string)(string, error) {
	w, err1 := strconv.Atoi(rec[1])
	l, err2 := strconv.Atoi(rec[2])
	var err error

	if err1 != nil {
		err = err1 
	} else if err2 != nil {
		err = err2
	}
			
	return strconv.Itoa(w-l), err
}



// Mapping (move to json)
var h1  = []string{"team","wins","losses"}

var h2  = []string{"team", "win diff1", "win diff2","win diff3","win diff4","win diff5","win diff6","win diff7"}

var trans = map[string]func([]string)(string, error){
	"team": isOakland,
	"win diff1": winDiff,
	"win diff2": winDiff,
	"win diff3": winDiff,
	"win diff4": winDiff,
	"win diff5": winDiff,
	"win diff6": winDiff,
	"win diff7": winDiff,
}



func main() {
	m:= csvutils.Mapping{Inhdr: h1, Outhdr: h2, Mapper: trans}
	inpath := "./test.csv"
	outpath := "./test.out.csv"
	errpath := "./test.err.csv"

	csvutils.ProcessLines(inpath, outpath, errpath, m)
}

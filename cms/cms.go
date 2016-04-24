// CMS module look-up file run=123 dataset=/abs/*/*
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet@gmail.com>
package cms

import (
	"fmt"
	"github.com/vkuznet/dbsfiles/utils"
	"time"
)

// global variables
var DBSINFO bool

func dbsUrl() string {
	return "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
}

// main record we work with
type Record map[string]interface{}

// exported function which process user request
func Process(dataset string, run int) {
	startTime := time.Now()
	utils.TestEnv()
	datasets := listDatasets(dataset, run)
	if utils.VERBOSE > 0 {
		fmt.Printf("Found %d datasets\n", len(datasets))
	}
	ch := make(chan Record)
	for _, rec := range datasets {
		dataset := rec["dataset"].(string)
		go fileInfo(dataset, run, ch)
	}
	// collect results
	var out []Record
	count := 0
	for {
		select {
		case r := <-ch:
			if _, ok := r["done"]; ok == true { // look-up done record and count
				count += 1
			} else {
				out = append(out, r)
			}
		default:
			time.Sleep(time.Duration(10) * time.Millisecond) // wait for response
		}
		if len(datasets) == count {
			break
		}
	}
	for _, rec := range out {
		fmt.Println(rec["logical_file_name"])
	}
	if utils.PROFILE {
		fmt.Printf("Processed %d urls\n", utils.UrlCounter)
		fmt.Printf("Number of files: %d\n", len(out))
		fmt.Printf("Elapsed time %s\n", time.Since(startTime))
	}
}

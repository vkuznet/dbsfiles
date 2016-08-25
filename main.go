// dbsfiles tool aggregates statistics from CMS popularity DB, DBS, SiteDB
// and presents results for any given tier site and time interval
package main

import (
	"flag"
	"github.com/vkuznet/dbsfiles/cms"
	"github.com/vkuznet/dbsfiles/utils"
)

func main() {
	var dataset string
	flag.StringVar(&dataset, "dataset", "", "dataset name")
	//     var trange string
	//     flag.StringVar(&trange, "trange", "1d", "Specify time interval in YYYYMMDD format, e.g 20150101-20150201 or use short notations 1d, 1m, 1y for one day, month, year, respectively")
	var run int
	flag.IntVar(&run, "run", 0, "run number")
	var numFiles int
	flag.IntVar(&numFiles, "numFiles", 10, "show first #files")
	var chunkSize int
	flag.IntVar(&chunkSize, "chunkSize", 100, "chunkSize for processing URLs")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "Verbose level, support 0,1,2")
	var profile bool
	flag.BoolVar(&profile, "profile", false, "profile code")
	flag.Parse()
	utils.VERBOSE = verbose
	utils.PROFILE = profile
	utils.CHUNKSIZE = chunkSize
	cms.Process(dataset, run, numFiles)
}

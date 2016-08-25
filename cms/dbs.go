package cms

import (
	"encoding/json"
	"fmt"
	"github.com/vkuznet/dbsfiles/utils"
	"net/url"
)

// helper function to load DBS data stream
func loadDBSData(furl string, data []byte) []Record {
	var out []Record
	err := json.Unmarshal(data, &out)
	if err != nil {
		if utils.VERBOSE > 0 {
			msg := fmt.Sprintf("DBS unable to unmarshal the data, furl=%s, data=%s, error=%v", furl, string(data), err)
			fmt.Println(msg)
		}
		return out
	}
	return out
}

// DBS helper function to get dataset info from blocksummaries DBS API
func fileInfo(dataset string, run int, ch chan Record) {
	api := "files"
	var furl string
	if run != 0 {
		furl = fmt.Sprintf("%s/%s/?dataset=%s&run_num=%d", dbsUrl(), api, url.QueryEscape(dataset), run)
	} else {
		furl = fmt.Sprintf("%s/%s/?dataset=%s", dbsUrl(), api, url.QueryEscape(dataset))
	}
	response := utils.FetchResponse(furl, "")
	if response.Error == nil {
		records := loadDBSData(furl, response.Data)
		if utils.VERBOSE > 1 {
			fmt.Println("furl", furl, records)
		}
		if utils.VERBOSE > 0 {
			nfiles := len(records)
			if utils.NUMFILES != 0 {
				nfiles = utils.NUMFILES
			}
			fmt.Printf("Total number of files %d, showing %d:\n", len(records), nfiles)
		}
		for idx, rec := range records {
			if utils.NUMFILES != 0 {
				if idx > utils.NUMFILES {
					break
				}
			}
			ch <- rec
		}
	}
	rec := make(Record)
	rec["done"] = 1
	ch <- rec // done record
}

// DBS helper function to get dataset info from blocksummaries DBS API
func listDatasets(dataset string, run int) []Record {
	api := "datasets"
	var furl string
	if run != 0 {
		furl = fmt.Sprintf("%s/%s/?dataset=%s&run_num=%d", dbsUrl(), api, url.QueryEscape(dataset), run)
	} else {
		furl = fmt.Sprintf("%s/%s/?dataset=%s", dbsUrl(), api, url.QueryEscape(dataset))
	}
	response := utils.FetchResponse(furl, "")
	if response.Error == nil {
		records := loadDBSData(furl, response.Data)
		if utils.VERBOSE > 1 {
			fmt.Println("furl", furl, records)
		}
		return records
	}
	var out []Record
	return out
}

// helper function to get CMS data tier names
func dataTiers() []string {
	var out []string
	api := "datatiers"
	furl := fmt.Sprintf("%s/%s/", dbsUrl(), api)
	response := utils.FetchResponse(furl, "")
	if response.Error == nil {
		records := loadDBSData(furl, response.Data)
		if utils.VERBOSE > 1 {
			fmt.Println("furl", furl, records)
		}
		for _, rec := range records {
			tier := rec["data_tier_name"].(string)
			out = append(out, tier)
		}
	}
	return utils.List2Set(out)

}

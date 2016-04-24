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
	furl := fmt.Sprintf("%s/%s/?dataset=%s&run_num=%d", dbsUrl(), api, url.QueryEscape(dataset), run)
	response := utils.FetchResponse(furl, "")
	if response.Error == nil {
		records := loadDBSData(furl, response.Data)
		if utils.VERBOSE > 1 {
			fmt.Println("furl", furl, records)
		}
		for _, rec := range records {
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
	furl := fmt.Sprintf("%s/%s/?dataset=%s&run_num=%d", dbsUrl(), api, dataset, run)
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

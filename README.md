# dbsfiles

[![Build Status](https://travis-ci.org/vkuznet/dbsfiles.svg?branch=master)](https://travis-ci.org/vkuznet/dbsfiles)
[![GoDoc](https://godoc.org/github.com/vkuznet/dbsfiles?status.svg)](https://godoc.org/github.com/vkuznet/dbsfiles)

### dbsfiles tool
dbsfiles tool designed to get list of files from DBS for provided set of
conditions. So far only dataset pattern and run number is supported.

### Build
to build the tool either use
```
go build
```
or
```
make
```

### Usage
```
dbsfiles -dataset "/*/*/*" -run 207889
```

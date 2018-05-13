package wal

import (
	"fmt"
	"io/ioutil"
	"sort"
	"strings"

	"github.com/golang/glog"
)

type filenameInfo struct {
	name  string
	seqNo int64
}

func listLogFiles(dirname string) ([]filenameInfo, error) {
	fis, err := ioutil.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	parsedNames := make([]filenameInfo, 0, len(fis))
	for _, fi := range fis {
		name := fi.Name()
		if !(strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log")) {
			glog.Warningf("Skipping file %v in WAL directory, does not appear to be a WAL file.", name)
			continue
		}

		pn, err := parseFilename(name)
		if err != nil {
			return nil, err
		}
		parsedNames = append(parsedNames, pn)
	}

	sort.Slice(parsedNames, func(i, j int) bool {
		return parsedNames[i].seqNo < parsedNames[j].seqNo
	})

	return parsedNames, nil
}

func parseFilename(n string) (filenameInfo, error) {
	var seqNo int64
	if _, err := fmt.Sscanf(n, "wal-%d.log", &seqNo); err != nil {
		return filenameInfo{}, err
	}

	return filenameInfo{
		name:  n,
		seqNo: seqNo,
	}, nil
}

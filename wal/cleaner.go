package wal

import (
	"os"
	"path/filepath"

	"github.com/golang/glog"
)

// CleanUnusedFiles cleans unused log files, i.e those that have already been applied.
func CleanUnusedFiles(dirname string, appliedUntil int64) {
	parsedNames, err := listLogFiles(dirname)
	if err != nil {
		glog.Warningf("error listing log files: %v", err)
		return
	}

	cleaned := 0
	for i, pn := range parsedNames {
		if pn.seqNo < appliedUntil && i > 0 {
			// can delete *previous* logfile, which spans
			// [parsedNames[i-1].seqNo, parsedNames[i].seqNo)
			fullFn := filepath.Join(dirname, parsedNames[i-1].name)
			glog.V(2).Infof("deleting unused log file %v", fullFn)

			if err := os.Remove(fullFn); err != nil {
				glog.Warningf("error while removing unused logfile %v: %v", fullFn, err)
			} else {
				cleaned++
			}
		}
	}

	if cleaned > 0 {
		glog.Infof("cleaned %v unused log files", cleaned)
	}
}

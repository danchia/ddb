package server

import (
	"html/template"
	"net"
	"net/http"
	"sync"

	"go.opencensus.io/trace"

	"github.com/golang/glog"
)

// Traces handles debug requests calls.
func Traces(w http.ResponseWriter, req *http.Request) {
	if !isAllowed(req) {
		http.Error(w, "Unauthorized.", http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "text/html")

	data := &traceDisplay{
		Summary: trace.SampledSpansSummary(),
	}

	if err := traceTmpl().ExecuteTemplate(w, "trace", data); err != nil {
		glog.Warningf("trace debug failed execute template: %v", err)
	}
}

type traceDisplay struct {
	Summary map[string]trace.PerMethodSummary
}

func isAllowed(r *http.Request) bool {
	h, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		h = r.RemoteAddr
	}
	switch h {
	case "localhost", "::1", "127.0.0.1":
		return true
	default:
		return false
	}
}

var traceTmplCache *template.Template
var traceTemplateOnce sync.Once

func traceTmpl() *template.Template {
	traceTemplateOnce.Do(func() {
		traceTmplCache = template.Must(template.New("trace").Parse(traceHTML))
	})
	return traceTmplCache
}

const traceHTML = `
<html>
	<head><title>ddebug/requests</title></head>
	<body>
		<table>
		{{ range $fam, $summary := .Summary }}
			<tr>
				<td>
				{{$fam}}
				</td>
				<td>
					{{ $summary.Active }} active
				</td>
				{{ range $lb := $summary.LatencyBuckets }}
					<td>
						[{{ $lb.MinLatency }} - {{ $lb.MaxLatency }}]
					</td>
				{{ end }}
			</tr>
		{{ end }}
		</table>
	</body>
</html>
`

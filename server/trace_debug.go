package server

import (
	"html/template"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.opencensus.io/trace"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/glog"
)

// Traces handles debug requests calls.
// Inspired heavily by golang.org/x/net/trace/trace.go
func Traces(w http.ResponseWriter, req *http.Request) {
	if !isAllowed(req) {
		http.Error(w, "Unauthorized.", http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	data := &traceDisplay{
		Summary: trace.SampledSpansSummary(),
	}
	if req.FormValue("fam") != "" {
		data.DisplayFamily = req.FormValue("fam")
		bucket, err := strconv.Atoi(req.FormValue("b"))
		if err != nil {
			glog.Warningf("trace debug failed parse arg: $v", err)
			return
		}
		if bucket == -1 {
			data.MinLatency = 0
			data.Spans = trace.ActiveSpans(data.DisplayFamily)
		} else {
			methodSummary := data.Summary[data.DisplayFamily]
			bs := methodSummary.LatencyBuckets[bucket]
			data.MinLatency = bs.MinLatency
			data.Spans = trace.LatencySampledSpans(data.DisplayFamily, bs.MinLatency, bs.MaxLatency)
		}
	}

	glog.V(2).Infof("data: %v", data)

	if err := traceTmpl().ExecuteTemplate(w, "trace", data); err != nil {
		glog.Warningf("trace debug failed execute template: %v", err)
	}
}

type traceDisplay struct {
	Summary map[string]trace.PerMethodSummary

	DisplayFamily string
	MinLatency    time.Duration
	Spans         []*trace.SpanData
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
		traceTmplCache = template.Must(template.New("trace").Funcs(template.FuncMap{
			"sdump": spew.Sdump,
		}).Parse(traceHTML))
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
					<a href="?fam={{$fam}}&b=-1">
					{{ $summary.Active }} active
					</a>
				</td>
				{{ range $i, $lb := $summary.LatencyBuckets }}
					<td>
						<a href="?fam={{$fam}}&b={{$i}}">
						[{{ $lb.MinLatency }} - {{ $lb.MaxLatency }} ({{$lb.Size}})]
						</a>
					</td>
				{{ end }}
			</tr>
		{{ end }}
		</table>
		{{ if .DisplayFamily }}
			<h2>{{.DisplayFamily}} {{.MinLatency}}</h2>
			{{ range $span := .Spans }}

				<p style="font-family: monospace">
					<b>{{ $span.Name }}</b>. TraceID: {{$span.TraceID}} SpanID: {{$span.SpanID}} ParentSpanID: {{$span.ParentSpanID}}<br>
					{{ $span.Attributes }} <br>
					{{ $span.StartTime }} Start<br>
					{{ range $ann := $span.Annotations }}
					{{ $ann.Time }} {{ $ann.Message }} {{ $ann.Attributes }}<br>
					{{ end }}
					{{ $span.EndTime }} Finish with Status {{ $span.Status.Code }}. {{ $span.Status.Message }}
				</p>

			{{ end }}
		{{ end }}
	</body>
</html>
`

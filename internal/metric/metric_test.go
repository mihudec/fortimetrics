package metric

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestLine(t *testing.T) {
	m := Metric{Measurement: "vs status", Tags: map[string]string{"name": "a,b=c"}, Fields: map[string]any{"count": json.Number("9007199254740993"), "status": "a\"b\\c"}, Timestamp: time.Unix(1, 0)}
	line, e := m.Line()
	if e != nil {
		t.Fatal(e)
	}
	want := `vs\ status,name=a\,b\=c count=9007199254740993i,status="a\"b\\c" 1000000000`
	if line != want {
		t.Fatalf("got %s, want %s", line, want)
	}
	m.Tags["name"] = "bad\nline"
	if _, e = m.Line(); e == nil {
		t.Fatal("accepted newline")
	}
	m.Tags["name"] = "ok"
	m.Fields["count"] = json.Number("NaN")
	if _, e = m.Line(); e == nil {
		t.Fatal("accepted NaN")
	}
	m.Fields["count"] = json.Number("18446744073709551615")
	line, e = m.Line()
	if e != nil || !strings.Contains(line, "18446744073709551615u") {
		t.Fatal(line, e)
	}
}

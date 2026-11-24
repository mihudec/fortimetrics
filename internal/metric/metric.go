package metric

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Metric is the sink-independent value passed from collectors to publishers.
type Metric struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]any
	Timestamp   time.Time
}

func escape(s string, equals bool) string {
	r := strings.NewReplacer("\\", "\\\\", ",", "\\,", " ", "\\ ")
	s = r.Replace(s)
	if equals {
		s = strings.ReplaceAll(s, "=", "\\=")
	}
	return s
}
func clean(s string) bool { return !strings.ContainsAny(s, "\r\n") }
func (m Metric) Line() (string, error) {
	if m.Measurement == "" || !clean(m.Measurement) || len(m.Fields) == 0 || m.Timestamp.IsZero() {
		return "", fmt.Errorf("invalid metric")
	}
	var b strings.Builder
	b.WriteString(escape(m.Measurement, false))
	keys := make([]string, 0, len(m.Tags))
	for k := range m.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := m.Tags[k]
		if k == "" || !clean(k) || !clean(v) {
			return "", fmt.Errorf("invalid tag")
		}
		if v != "" {
			fmt.Fprintf(&b, ",%s=%s", escape(k, true), escape(v, true))
		}
	}
	keys = keys[:0]
	for k := range m.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		if k == "" || !clean(k) {
			return "", fmt.Errorf("invalid field")
		}
		if i == 0 {
			b.WriteByte(' ')
		} else {
			b.WriteByte(',')
		}
		b.WriteString(escape(k, true))
		b.WriteByte('=')
		switch v := m.Fields[k].(type) {
		case string:
			if !clean(v) {
				return "", fmt.Errorf("newline in field")
			}
			b.WriteString("\"" + strings.NewReplacer("\\", "\\\\", "\"", "\\\"").Replace(v) + "\"")
		case bool:
			b.WriteString(strconv.FormatBool(v))
		case int64:
			b.WriteString(strconv.FormatInt(v, 10) + "i")
		case json.Number:
			if n, e := v.Int64(); e == nil {
				b.WriteString(strconv.FormatInt(n, 10) + "i")
			} else if n, e := strconv.ParseUint(string(v), 10, 64); e == nil {
				b.WriteString(strconv.FormatUint(n, 10) + "u")
			} else {
				f, e := v.Float64()
				if e != nil || math.IsNaN(f) || math.IsInf(f, 0) {
					return "", fmt.Errorf("invalid number")
				}
				b.WriteString(strconv.FormatFloat(f, 'g', -1, 64))
			}
		default:
			return "", fmt.Errorf("unsupported field type %T", v)
		}
	}
	fmt.Fprintf(&b, " %d", m.Timestamp.UnixNano())
	return b.String(), nil
}

package logrec

import (
	"strings"
	"testing"
	"time"
)

const tenant = "3f0c1b2a-8d4e-4b6f-9a1c-2e5d7f8a9b0c"

func TestDecodeSingleObject(t *testing.T) {
	body := `{"tenant_id":"` + tenant + `","ts":"2026-09-22T10:11:12.123456789Z","level":"warn","source":"api","message":"disk almost full","attrs":{"disk":"/dev/sda1","pct":91}}`

	recs, err := Decode(strings.NewReader(body))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	r := recs[0]
	if r.TenantID.String() != tenant {
		t.Errorf("tenant_id = %q, want %q", r.TenantID, tenant)
	}
	want := time.Date(2026, 9, 22, 10, 11, 12, 123456789, time.UTC)
	if !r.TS.Equal(want) {
		t.Errorf("ts = %v, want %v (nanosecond precision must survive)", r.TS, want)
	}
	if r.Level != "warn" || r.Source != "api" || r.Message != "disk almost full" {
		t.Errorf("unexpected fields: %+v", r)
	}
	if !strings.Contains(string(r.Attrs), `"pct":91`) {
		t.Errorf("attrs = %s, want the original JSON object", r.Attrs)
	}
}

func TestDecodeArray(t *testing.T) {
	body := `[
	  {"tenant_id":"` + tenant + `","ts":"2026-09-22T10:00:00Z","message":"one"},
	  {"tenant_id":"` + tenant + `","ts":"2026-09-22T10:00:01Z","message":"two"}
	]`

	recs, err := Decode(strings.NewReader(body))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if recs[0].Message != "one" || recs[1].Message != "two" {
		t.Errorf("order not preserved: %q, %q", recs[0].Message, recs[1].Message)
	}
	if recs[0].Attrs != nil {
		t.Errorf("absent attrs = %v, want nil so the column stays NULL", recs[0].Attrs)
	}
}

func TestDecodeRejects(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		field string
	}{
		{"empty body", ``, ""},
		{"not json", `not json`, ""},
		{"empty array", `[]`, ""},
		{"missing tenant_id", `{"ts":"2026-09-22T10:00:00Z"}`, "tenant_id"},
		{"blank tenant_id", `{"tenant_id":"","ts":"2026-09-22T10:00:00Z"}`, "tenant_id"},
		{"tenant_id not a uuid", `{"tenant_id":"nope","ts":"2026-09-22T10:00:00Z"}`, "tenant_id"},
		{"missing ts", `{"tenant_id":"` + tenant + `"}`, "ts"},
		{"ts not rfc3339", `{"tenant_id":"` + tenant + `","ts":"22/09/2026 10:00"}`, "ts"},
		{"ts without timezone", `{"tenant_id":"` + tenant + `","ts":"2026-09-22T10:00:00"}`, "ts"},
		{"attrs not an object", `{"tenant_id":"` + tenant + `","ts":"2026-09-22T10:00:00Z","attrs":[1,2]}`, "attrs"},
		{"one bad item in array", `[{"tenant_id":"` + tenant + `","ts":"2026-09-22T10:00:00Z"},{"tenant_id":"bad","ts":"2026-09-22T10:00:00Z"}]`, "tenant_id"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recs, err := Decode(strings.NewReader(tc.body))
			if err == nil {
				t.Fatalf("Decode accepted %q", tc.body)
			}
			if recs != nil {
				t.Errorf("got %d records alongside the error, want none (all-or-nothing)", len(recs))
			}
			var verr *Error
			if !errorAs(err, &verr) {
				t.Fatalf("error type = %T, want *logrec.Error", err)
			}
			if tc.field != "" && verr.Field != tc.field {
				t.Errorf("field = %q, want %q", verr.Field, tc.field)
			}
		})
	}
}

func TestDecodeNullAttrs(t *testing.T) {
	body := `{"tenant_id":"` + tenant + `","ts":"2026-09-22T10:00:00Z","attrs":null}`
	recs, err := Decode(strings.NewReader(body))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if recs[0].Attrs != nil {
		t.Errorf("attrs = %v, want nil", recs[0].Attrs)
	}
}

func errorAs(err error, target **Error) bool {
	e, ok := err.(*Error)
	if ok {
		*target = e
	}
	return ok
}

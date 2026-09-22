// Package logrec defines the log record payload and its decoding/validation.
package logrec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
)

// Record is a single validated log line ready to be written to Postgres.
type Record struct {
	TenantID uuid.UUID
	TS       time.Time
	Level    string
	Source   string
	Message  string
	Attrs    []byte // raw JSON object, nil when absent
}

// wire is the on-the-wire shape of a log record. Pointers distinguish an
// absent field from an empty one.
type wire struct {
	TenantID *string         `json:"tenant_id"`
	TS       *string         `json:"ts"`
	Level    string          `json:"level"`
	Source   string          `json:"source"`
	Message  string          `json:"message"`
	Attrs    json.RawMessage `json:"attrs"`
}

// Error describes why a payload was rejected. Index is the position in the
// submitted array, or 0 for a single object.
type Error struct {
	Index int
	Field string
	Msg   string
}

func (e *Error) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("item %d: %s", e.Index, e.Msg)
	}
	return fmt.Sprintf("item %d: field %q: %s", e.Index, e.Field, e.Msg)
}

// Decode reads a single log object or an array of them and validates every
// record. It is all-or-nothing: if any record is invalid, no records are
// returned.
func Decode(r io.Reader) ([]Record, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) == 0 {
		return nil, &Error{Msg: "empty request body"}
	}

	var items []wire
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &items); err != nil {
			return nil, &Error{Msg: "invalid JSON array: " + err.Error()}
		}
	} else {
		var single wire
		if err := json.Unmarshal(trimmed, &single); err != nil {
			return nil, &Error{Msg: "invalid JSON object: " + err.Error()}
		}
		items = []wire{single}
	}
	if len(items) == 0 {
		return nil, &Error{Msg: "payload contains no log records"}
	}

	recs := make([]Record, 0, len(items))
	for i, it := range items {
		rec, err := it.validate(i)
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

func (w wire) validate(idx int) (Record, error) {
	if w.TenantID == nil || *w.TenantID == "" {
		return Record{}, &Error{Index: idx, Field: "tenant_id", Msg: "is required"}
	}
	id, err := uuid.Parse(*w.TenantID)
	if err != nil {
		return Record{}, &Error{Index: idx, Field: "tenant_id", Msg: "must be a UUID"}
	}
	if w.TS == nil || *w.TS == "" {
		return Record{}, &Error{Index: idx, Field: "ts", Msg: "is required"}
	}
	ts, err := time.Parse(time.RFC3339Nano, *w.TS)
	if err != nil {
		return Record{}, &Error{Index: idx, Field: "ts", Msg: "must be an RFC3339Nano timestamp"}
	}

	attrs := bytes.TrimSpace(w.Attrs)
	switch {
	case len(attrs) == 0 || bytes.Equal(attrs, []byte("null")):
		attrs = nil
	case attrs[0] != '{':
		return Record{}, &Error{Index: idx, Field: "attrs", Msg: "must be a JSON object"}
	}

	return Record{
		TenantID: id,
		TS:       ts,
		Level:    w.Level,
		Source:   w.Source,
		Message:  w.Message,
		Attrs:    attrs,
	}, nil
}

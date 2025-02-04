package warc

import (
	"testing"

	"github.com/slyrz/warc"
)

func TestCapture(t *testing.T) {
	base, err := ResourceRecord([]byte{}, "qwerty", "text/plain")
	if err != nil {
		t.Fatal(err.Error())
	}
	concurrent1, err := ResourceRecord([]byte{}, "qwerty", "text/plain")
	if err != nil {
		t.Fatal(err.Error())
	}
	concurrent2, err := ResourceRecord([]byte{}, "qwerty", "text/plain")
	if err != nil {
		t.Fatal(err.Error())
	}

	concurrents := []*warc.Record{concurrent1, concurrent2}
	Capture(base, concurrents)

	for _, c := range concurrents {
		baseId := base.Header.Get(string(WarcRecordId))
		baseDate := base.Header.Get(string(WarcDate))
		concurrentDate := c.Header.Get(string(WarcDate))

		to := c.Header.Get(string(WarcConcurrentTo))

		if baseId != to {
			t.Errorf("Unexpected Warc-Concurrent-To header. Have: %s, want: %s", to, baseId)
		}
		if baseDate != concurrentDate {
			t.Errorf("Unexpected Warc-Data header. Have: %s, want: %s", concurrentDate, baseDate)
		}
	}
}

package warc

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/google/uuid"
	"github.com/slyrz/warc"
)

type WarcTypeField string

var (
	Resource     WarcTypeField = "resource"
	Metadata     WarcTypeField = "metadata"
	Request      WarcTypeField = "request"
	Revisit      WarcTypeField = "revisit"
	Response     WarcTypeField = "response"
	Continuation WarcTypeField = "continuation"
	Info         WarcTypeField = "warcinfo"
)

type WarcHeader string

var (
	WarcType                  WarcHeader = "warc-type"
	WarcRecordId              WarcHeader = "warc-record-id"
	WarcDate                  WarcHeader = "warc-date"
	ContentLength             WarcHeader = "content-length"
	ContentType               WarcHeader = "content-type"
	WarcConcurrentTo          WarcHeader = "warc-concurrent-to"
	WarcBlockDigest           WarcHeader = "warc-block-digest"
	WarcPayloadDigest         WarcHeader = "warc-payload-digest"
	WarcIpAddress             WarcHeader = "warc-ip-address"
	WarcRefersTo              WarcHeader = "warc-refers-to"
	WarcTargetURI             WarcHeader = "warc-target-uri"
	WarcTruncated             WarcHeader = "warc-truncated"
	WarcWarcinfoID            WarcHeader = "warc-warcinfo-id"
	WarcFilename              WarcHeader = "warc-filename"
	WarcProfile               WarcHeader = "warc-profile"
	WarcIdentifiedPayloadType WarcHeader = "warc-identified-payload-type"
	WarcSegmentOriginID       WarcHeader = "warc-segment-origin-id"
	WarcSegmentNumber         WarcHeader = "warc-segment-number"
	WarcSegmentTotalLength    WarcHeader = "warc-segment-total-length"
)

func newRecord(warctype WarcTypeField, data []byte) (*warc.Record, error) {
	record := warc.NewRecord()
	id, err := makeRecordId()
	if err != nil {
		return nil, err
	}

	record.Header.Set(string(WarcRecordId), id)
	record.Header.Set(string(WarcType), string(warctype))
	record.Header.Set(string(WarcDate), time.Now().Format("2006-01-02T15:04:05Z"))
	record.Content = bytes.NewReader(data)
	return record, nil
}

func fieldsToBytes(fields map[string]string) []byte {
	var bytes []byte
	for k, v := range fields{
		field := fmt.Sprintf("%s: %s\r\n", k, v)
		bytes = append(bytes, []byte(field)...)
	}
	return bytes
}

func makeRecordId() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("<urn:uuid:%s>", id.String()), nil
}

func RequestRecord(req *http.Request) (*warc.Record, error) {
	bytes, err := httputil.DumpRequestOut(req, true)
	if err != nil {
		return nil, err
	}
	record, err := newRecord(Request, bytes)

	if err != nil {
		return nil, err
	}

	record.Header.Set(string(ContentType), "application/http;msgtype=request")
	record.Header.Set(string(WarcTargetURI), req.URL.String())
	return record, nil
}

func ResponseRecord(res *http.Response) (*warc.Record, error) {
	bytes, err := httputil.DumpResponse(res, true)
	if err != nil {
		println("here")
		return nil, err
	}
	record, err := newRecord(Response, bytes)

	if err != nil {
		return nil, err
	}

	record.Header.Set(string(ContentType), "application/http;msgtype=request")
	record.Header.Set(string(WarcTargetURI), res.Request.URL.String())
	return record, nil
}

func ResourceRecord(data []byte, target string, mime string) (*warc.Record, error) {
	record, err := newRecord(Resource, data)

	if err != nil {
		return nil, err
	}

	record.Header.Set(string(ContentType), mime)
	record.Header.Set(string(WarcTargetURI), target)
	return record, nil
}

func WarcinfoRecord(fields map[string]string) (*warc.Record, error) {
	record, err := newRecord(Info, fieldsToBytes(fields))

	if err != nil {
		return nil, err
	}

	record.Header.Set(string(ContentType), "application/warc-fields")
	return record, nil
}

func MetadataRecord(fields map[string]string, target string) (*warc.Record, error) {
	record, err := newRecord(Metadata, fieldsToBytes(fields))

	if err != nil {
		return nil, err
	}

	record.Header.Set(string(ContentType), "application/warc-fields")
	record.Header.Set(string(WarcTargetURI), target)
	return record, nil
}

func Capture(base *warc.Record, concurrent []*warc.Record){
	for _, r := range concurrent {

		r.Header.Set(string(WarcDate), base.Header.Get(string(WarcDate)))
		r.Header.Set(string(WarcConcurrentTo), base.Header.Get(string(WarcRecordId)))
	}
}

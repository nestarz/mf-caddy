package mfcache

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
)

// capture forwards a response to the client as it is written, adds this cache's Cache-Status
// member, and keeps a copy of the body while the response may still be stored.
type capture struct {
	http.ResponseWriter
	member  string
	decide  func(status int, header http.Header) (suffix string, keep bool)
	limit   int
	status  int
	length  int64
	keep    bool
	failed  bool
	body    bytes.Buffer
	sink    io.Writer
	copied  int64
	written bool
}

// newCapture wraps w; decide sees the final status and headers and says what to add to member and
// whether to keep the body.
func newCapture(w http.ResponseWriter, member string, decide func(int, http.Header) (string, bool)) *capture {
	return &capture{ResponseWriter: w, member: member, decide: decide, length: -1}
}

// WriteHeader passes informational responses through and decides on the final one.
func (c *capture) WriteHeader(status int) {
	if c.written {
		return
	}
	if status >= 100 && status < 200 && status != http.StatusSwitchingProtocols {
		c.ResponseWriter.WriteHeader(status)
		return
	}
	c.written = true
	c.status = status
	header := c.ResponseWriter.Header()
	if n, err := strconv.ParseInt(header.Get("Content-Length"), 10, 64); err == nil {
		c.length = n
	}
	suffix, keep := c.decide(status, header)
	c.keep = keep
	member := c.member
	if suffix != "" {
		member += "; " + suffix
	}
	header.Add("Cache-Status", member)
	c.ResponseWriter.WriteHeader(status)
}

// Write forwards b and copies it while the body still fits the entry limit.
func (c *capture) Write(b []byte) (int, error) {
	if !c.written {
		c.WriteHeader(http.StatusOK)
	}
	if c.keep {
		if c.copied+int64(len(b)) > int64(c.limit) {
			c.keep = false
			c.body = bytes.Buffer{}
		} else {
			var err error
			var written int
			if c.sink != nil {
				written, err = c.sink.Write(b)
			} else {
				written, err = c.body.Write(b)
			}
			if err != nil || written != len(b) {
				c.keep = false
			} else {
				c.copied += int64(len(b))
			}
		}
	}
	n, err := c.ResponseWriter.Write(b)
	if err != nil || n != len(b) {
		c.failed = true
	}
	return n, err
}

// Flush sends what was written so far, so streamed responses stay streamed.
func (c *capture) Flush() {
	if !c.written {
		c.WriteHeader(http.StatusOK)
	}
	if err := http.NewResponseController(c.ResponseWriter).Flush(); err != nil {
		c.failed = true
	}
}

// Unwrap gives http.ResponseController the underlying writer.
func (c *capture) Unwrap() http.ResponseWriter {
	return c.ResponseWriter
}

// finish writes the status of a handler that returned without writing.
func (c *capture) finish() {
	if !c.written {
		c.WriteHeader(http.StatusOK)
	}
}

// complete reports that the whole body reached the client and is held.
func (c *capture) complete() bool {
	if !c.keep || c.failed {
		return false
	}
	return c.length < 0 || c.copied == c.length
}

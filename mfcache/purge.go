package mfcache

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func (h *Handler) validatePurge() error {
	if h.PurgePath == "" && h.PurgeTokenHash == "" {
		return nil
	}
	hash, err := hex.DecodeString(h.PurgeTokenHash)
	if h.Scope == "" || !strings.HasPrefix(h.PurgePath, "/") || err != nil || len(hash) != sha256.Size {
		return fmt.Errorf("purging requires scope, an absolute purge_path and a SHA-256 purge_token_hash")
	}
	return nil
}

// purge acknowledges only a completed local purge; the fleet API coordinates all ingress hosts.
func (h *Handler) purge(w http.ResponseWriter, r *http.Request) error {
	w.Header().Set("Cache-Control", "no-store")
	expected, err := hex.DecodeString(h.PurgeTokenHash)
	token, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	actual := sha256.Sum256([]byte(token))
	if !bearer || token == "" || err != nil || len(expected) != sha256.Size || subtle.ConstantTimeCompare(actual[:], expected) != 1 {
		http.Error(w, "not found", http.StatusNotFound)
		return nil
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return nil
	}
	var body struct {
		Tags []string `json:"tags"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || len(body.Tags) == 0 || len(body.Tags) > 500 {
		http.Error(w, "expected 1 to 500 tags", http.StatusBadRequest)
		return nil
	}
	if decoder.Decode(new(any)) != io.EOF {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return nil
	}
	tags := make(map[string]bool, len(body.Tags))
	for _, tag := range body.Tags {
		if len(tag) == 0 || len(tag) > 256 || strings.ContainsAny(tag, ",\r\n\x00") || strings.TrimSpace(tag) != tag {
			http.Error(w, "invalid tag", http.StatusBadRequest)
			return nil
		}
		tags[tag] = true
	}
	h.store.purge(h.Scope, tags)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

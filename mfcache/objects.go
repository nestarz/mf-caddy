package mfcache

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ObjectStore moves bodies without buffering them. Put receives a bounded spool file, so signing
// and retries can seek instead of keeping another body-sized allocation.
type ObjectStore interface {
	Open(context.Context, string) (io.ReadCloser, int64, error)
	Put(context.Context, string, io.ReadSeeker, int64) error
	Delete(context.Context, string) error
}

type objectSummary struct {
	Key      string
	Modified time.Time
}
type objectLister interface {
	List(context.Context, string) ([]objectSummary, string, error)
}

// Listing is bounded and only used for recovery after a lost index, never for request lookup.
func (s *s3Objects) List(ctx context.Context, cursor string) ([]objectSummary, string, error) {
	input := &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &s.prefix, MaxKeys: aws.Int32(64)}
	if cursor != "" {
		input.ContinuationToken = &cursor
	}
	response, err := s.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, "", err
	}
	var objects []objectSummary
	for _, object := range response.Contents {
		key := strings.TrimPrefix(aws.ToString(object.Key), s.prefix)
		if len(key) == 48 {
			if _, err := hex.DecodeString(key); err == nil {
				objects = append(objects, objectSummary{key, aws.ToTime(object.LastModified)})
			}
		}
	}
	return objects, aws.ToString(response.NextContinuationToken), nil
}

// S3Connection is read from a systemd credential, never embedded in public Caddy configuration.
type S3Connection struct {
	Endpoint     string `json:"endpoint"`
	Bucket       string `json:"bucket"`
	Prefix       string `json:"prefix"`
	Region       string `json:"region"`
	PathStyle    bool   `json:"path_style"`
	AccessKey    string `json:"access_key"`
	SecretKey    string `json:"secret_key"`
	SessionToken string `json:"session_token,omitempty"`
	SPKI         string `json:"spki,omitempty"`
}

type cacheConnections struct {
	Private       S3Connection  `json:"private"`
	Public        *S3Connection `json:"public,omitempty"`
	PublicReadURL string        `json:"public_read_url,omitempty"`
}

type s3Objects struct {
	client         *s3.Client
	bucket, prefix string
}

func objectHTTPClient(pin string) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true // Stored bytes must not be transparently decompressed.
	transport.ResponseHeaderTimeout = 10 * time.Second
	transport.MaxResponseHeaderBytes = 64 << 10
	transport.MaxConnsPerHost = 80
	transport.MaxIdleConnsPerHost = 16
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if pin != "" {
		want, err := hex.DecodeString(pin)
		if err != nil || len(want) != sha256.Size {
			return nil, errors.New("invalid S3 SPKI pin")
		}
		// The explicit verifier accepts WebPKI OR this exact public key, including local self-signed
		// stores. InsecureSkipVerify alone is never used.
		transport.TLSClientConfig.InsecureSkipVerify = true
		transport.TLSClientConfig.VerifyConnection = func(cs tls.ConnectionState) error {
			cert := cs.PeerCertificates[0]
			got := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
			if hex.EncodeToString(got[:]) == pin {
				return nil
			}
			intermediates := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				intermediates.AddCert(c)
			}
			_, err := cert.Verify(x509.VerifyOptions{DNSName: cs.ServerName, Intermediates: intermediates})
			return err
		}
	}
	return &http.Client{Transport: transport, Timeout: 5 * time.Minute,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}

func newS3Objects(c S3Connection) (*s3Objects, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") || (u.Scheme != "https" && u.Scheme != "http") ||
		c.Bucket == "" || c.Region == "" || c.AccessKey == "" || c.SecretKey == "" {
		return nil, errors.New("invalid cache S3 connection")
	}
	if c.Prefix == "" || strings.HasPrefix(c.Prefix, "/") || strings.Contains(c.Prefix, "..") {
		return nil, errors.New("cache S3 prefix must be a dedicated relative namespace")
	}
	client, err := objectHTTPClient(c.SPKI)
	if err != nil {
		return nil, err
	}
	sdk := s3.New(s3.Options{Region: c.Region, BaseEndpoint: aws.String(c.Endpoint), UsePathStyle: c.PathStyle,
		Credentials: credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, c.SessionToken),
		HTTPClient:  client, RetryMaxAttempts: 2,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired})
	return &s3Objects{client: sdk, bucket: c.Bucket, prefix: strings.TrimSuffix(c.Prefix, "/") + "/"}, nil
}

func (s *s3Objects) Open(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	r, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key)})
	if err != nil {
		return nil, 0, err
	}
	return r.Body, aws.ToInt64(r.ContentLength), nil
}

func (s *s3Objects) Put(ctx context.Context, key string, body io.ReadSeeker, size int64) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key),
		Body: body, ContentLength: &size, ContentType: aws.String("application/octet-stream"),
		CacheControl: aws.String("public, max-age=86400, immutable")})
	return err
}

func (s *s3Objects) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key)})
	return err
}

func readConnections(path string) (cacheConnections, string, error) {
	var c cacheConnections
	f, err := os.Open(path)
	if err != nil {
		return c, "", err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil || len(b) > 64<<10 {
		return c, "", errors.New("invalid cache credential size")
	}
	if err = json.Unmarshal(b, &c); err != nil {
		return c, "", errors.New("invalid cache credential JSON")
	}
	digest := sha256.Sum256(b)
	return c, hex.EncodeToString(digest[:]), nil
}

// Public CDN reads are internal fetches of immutable bodies. Only this configured URL is used;
// visitor headers and credentials never reach it. A failed fetch can fall back before headers commit.
func openCDN(ctx context.Context, client *http.Client, base, key string, size int64) (io.ReadCloser, error) {
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, base+key, nil)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Accept-Encoding", "identity")
	response, err := client.Do(r)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 || response.ContentLength != size || response.Header.Get("Content-Encoding") != "" {
		response.Body.Close()
		return nil, fmt.Errorf("CDN object response mismatch: HTTP %d", response.StatusCode)
	}
	return response.Body, nil
}

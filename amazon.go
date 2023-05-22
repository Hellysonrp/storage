/*
Copyright The Helm Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package storage

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	pathutil "path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type s3ListObjectsFromDirectoryOutput struct {
	backend           *AmazonS3Backend
	prefix            string
	limit             int
	filesRead         []Metadata
	directoriesRead   []Metadata
	nextPageCalled    bool
	isEOF             bool
	continuationToken string
}

func (l *s3ListObjectsFromDirectoryOutput) GetDirectories() []Metadata {
	return l.directoriesRead
}

func (l *s3ListObjectsFromDirectoryOutput) GetFiles() []Metadata {
	return l.filesRead
}

func (l *s3ListObjectsFromDirectoryOutput) IsTruncated() bool {
	return !l.isEOF
}

func (l *s3ListObjectsFromDirectoryOutput) NextPage() (ListObjectsFromDirectoryOutput, error) {
	if l.nextPageCalled {
		return nil, errors.New("you cannot call NextPage more than once")
	}

	r := &s3ListObjectsFromDirectoryOutput{
		backend: l.backend,
		prefix:  l.prefix,
		limit:   l.limit,
	}

	if l.isEOF {
		r.isEOF = true
		return r, io.EOF
	}

	r.directoriesRead = make([]Metadata, 0, 5)
	r.filesRead = make([]Metadata, 0, 5)

	prefix := cleanPrefix(pathutil.Join(l.backend.Prefix, l.prefix))
	prefix = prefix + "/"

	req := &s3.ListObjectsV2Input{
		Bucket:    aws.String(l.backend.Bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(int64(l.limit)),
	}
	if l.continuationToken != "" {
		req.ContinuationToken = aws.String(l.continuationToken)
	}

	output, err := l.backend.Client.ListObjectsV2(req)

	if err != nil {
		return nil, err
	}

	if output.NextContinuationToken != nil {
		r.continuationToken = *output.NextContinuationToken
	}

	for _, d := range output.CommonPrefixes {
		if d.Prefix != nil && *d.Prefix != "" {
			p := removePrefixFromObjectPath(l.backend.Prefix, *d.Prefix)
			if !strings.HasPrefix(p, "/") {
				p = "/" + p
			}
			if p != "" && p != "/" {
				r.directoriesRead = append(r.directoriesRead, Metadata{
					Path: p,
				})
			}
		}
	}

	for _, f := range output.Contents {
		if f.Key != nil && *f.Key != "" {
			p := removePrefixFromObjectPath(l.backend.Prefix, *f.Key)
			if !strings.HasPrefix(p, "/") {
				p = "/" + p
			}
			if p != "" && p != "/" {
				m := Metadata{
					Path: p,
				}
				if f.LastModified != nil {
					m.LastModified = *f.LastModified
				}

				r.filesRead = append(r.filesRead, m)
			}
		}
	}

	r.isEOF = output.IsTruncated == nil || !*output.IsTruncated
	if r.isEOF {
		err = io.EOF
	}

	l.nextPageCalled = true

	return r, err
}

func (l *s3ListObjectsFromDirectoryOutput) FreeFromMemory() {
	l.directoriesRead = nil
	l.filesRead = nil
}

func (l *s3ListObjectsFromDirectoryOutput) Close() {
	l.FreeFromMemory()
}

// AmazonS3Backend is a storage backend for Amazon S3
type AmazonS3Backend struct {
	Bucket     string
	Client     *s3.S3
	Downloader *s3manager.Downloader
	Prefix     string
	Uploader   *s3manager.Uploader
	SSE        string
}

// NewAmazonS3Backend creates a new instance of AmazonS3Backend
func NewAmazonS3Backend(bucket string, prefix string, region string, endpoint string, sse string) *AmazonS3Backend {
	service := s3.New(session.New(), &aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		DisableSSL:       aws.Bool(strings.HasPrefix(endpoint, "http://")),
		S3ForcePathStyle: aws.Bool(endpoint != ""),
	})
	b := &AmazonS3Backend{
		Bucket:     bucket,
		Client:     service,
		Downloader: s3manager.NewDownloaderWithClient(service),
		Prefix:     cleanPrefix(prefix),
		Uploader:   s3manager.NewUploaderWithClient(service),
		SSE:        sse,
	}
	return b
}

// NewAmazonS3BackendWithCredentials creates a new instance of AmazonS3Backend with credentials
func NewAmazonS3BackendWithCredentials(bucket string, prefix string, region string, endpoint string, sse string, credentials *credentials.Credentials) *AmazonS3Backend {
	service := s3.New(session.New(), &aws.Config{
		Credentials:      credentials,
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		DisableSSL:       aws.Bool(strings.HasPrefix(endpoint, "http://")),
		S3ForcePathStyle: aws.Bool(endpoint != ""),
	})
	b := &AmazonS3Backend{
		Bucket:     bucket,
		Client:     service,
		Downloader: s3manager.NewDownloaderWithClient(service),
		Prefix:     cleanPrefix(prefix),
		Uploader:   s3manager.NewUploaderWithClient(service),
		SSE:        sse,
	}
	return b
}

// ListObjects lists all objects in Amazon S3 bucket, at prefix
func (b AmazonS3Backend) ListObjects(prefix string) ([]Object, error) {
	var objects []Object
	prefix = cleanPrefix(pathutil.Join(b.Prefix, prefix))
	s3Input := &s3.ListObjectsInput{
		Bucket: aws.String(b.Bucket),
		Prefix: aws.String(prefix),
	}
	for {
		s3Result, err := b.Client.ListObjects(s3Input)
		if err != nil {
			return objects, err
		}
		for _, obj := range s3Result.Contents {
			path := removePrefixFromObjectPath(prefix, *obj.Key)
			if objectPathIsInvalid(path) {
				continue
			}
			object := Object{
				Metadata: Metadata{
					Path:         path,
					LastModified: *obj.LastModified,
				},
				Content: []byte{},
			}
			objects = append(objects, object)
		}
		if !*s3Result.IsTruncated {
			break
		}
		s3Input.Marker = s3Result.Contents[len(s3Result.Contents)-1].Key
	}
	return objects, nil
}

// ListObjectsFromDirectory lists all objects under prefix, always with depth 1, returning at most limit objects (directories + files)
// It's intent is to abstract a directory listing
// Make sure prefix is a full path, other cases might give unexpected results
// If limit <= 0, it will return at most all the objects in 'prefix', limiting only by the backend limits
// You can know if the response is complete calling output.IsTruncated(), if true then the response isn't complete
func (b AmazonS3Backend) ListObjectsFromDirectory(prefix string, limit int) (ListObjectsFromDirectoryOutput, error) {
	s3Input := &s3.HeadObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(cleanPrefix(pathutil.Join(b.Prefix, prefix))),
	}

	_, err := b.Client.HeadObject(s3Input)
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() != "NotFound" {
			return nil, err
		}
	} else {
		return nil, ErrPrefixIsAnObject
	}

	output := &s3ListObjectsFromDirectoryOutput{
		prefix:  prefix,
		limit:   limit,
		backend: &b,
	}
	return output.NextPage()
}

func (b AmazonS3Backend) RenamePrefixOrObject(path, newPath string) error {
	// check if newPath is already occupied
	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(cleanPrefix(pathutil.Join(b.Prefix, newPath))),
	}

	_, err := b.Client.HeadObject(headObjectInput)
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if !ok || aerr.Code() != "NotFound" {
			return err
		}
	} else {
		return ErrNewPathNotEmpty
	}

	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(b.Bucket),
		Prefix:  aws.String(cleanPrefix(pathutil.Join(b.Prefix, newPath)) + "/"),
		MaxKeys: aws.Int64(1),
	}

	listObjectsOutput, err := b.Client.ListObjectsV2(listObjectsInput)
	if err != nil {
		return err
	}

	if listObjectsOutput.KeyCount != nil && *listObjectsOutput.KeyCount > 0 {
		return ErrNewPathNotEmpty
	}

	// check if path is an object or a prefix with objects
	headObjectInput = &s3.HeadObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(cleanPrefix(pathutil.Join(b.Prefix, path))),
	}
	_, err = b.Client.HeadObject(headObjectInput)
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if !ok || aerr.Code() != "NotFound" {
			return err
		}

		// is prefix with objects
		isEof := false
		var continuationToken string

		prefix := cleanPrefix(pathutil.Join(b.Prefix, path)) + "/"
		for !isEof {
			listObjectsInput = &s3.ListObjectsV2Input{
				Bucket: aws.String(b.Bucket),
				Prefix: aws.String(prefix),
			}
			if continuationToken != "" {
				listObjectsInput.ContinuationToken = aws.String(continuationToken)
			}

			listObjectsOutput, err = b.Client.ListObjectsV2(listObjectsInput)
			if err != nil {
				return err
			}

			if listObjectsOutput.ContinuationToken != nil {
				continuationToken = *listObjectsOutput.ContinuationToken
			}

			isEof = listObjectsOutput.IsTruncated == nil || !*listObjectsOutput.IsTruncated

			for _, obj := range listObjectsOutput.Contents {
				if obj.Key == nil || *obj.Key == "" {
					continue
				}

				key := removePrefixFromObjectPath(prefix, *obj.Key)
				if key == "" || key == "/" {
					continue
				}

				err = b.moveObject(*obj.Key, cleanPrefix(pathutil.Join(b.Prefix, newPath, key)))
				if err != nil {
					return err
				}
			}
		}
	} else {
		// is object
		err = b.moveObject(cleanPrefix(pathutil.Join(b.Prefix, path)), cleanPrefix(pathutil.Join(b.Prefix, newPath)))
		if err != nil {
			return err
		}
	}

	return nil
}

func (b AmazonS3Backend) moveObject(path string, newPath string) error {
	s3Input := &s3.CopyObjectInput{
		Bucket:     aws.String(b.Bucket),
		CopySource: aws.String(url.PathEscape(b.Bucket + "/" + path)),
		Key:        aws.String(newPath),
	}

	_, err := b.Client.CopyObject(s3Input)

	if err != nil {
		return err
	}

	return b.DeleteObject(path)
}

// GetObject retrieves an object from Amazon S3 bucket, at prefix
func (b AmazonS3Backend) GetObject(path string) (Object, error) {
	var object Object

	result, err := b.GetObjectStream(path)
	if err != nil {
		object.Path = path
		return object, err
	}
	defer result.Content.Close()

	object.Metadata = result.Metadata

	var content []byte
	content, err = ioutil.ReadAll(result.Content)
	if err != nil {
		return object, err
	}
	object.Content = content
	return object, nil
}

// PutObject uploads an object to Amazon S3 bucket, at prefix
func (b AmazonS3Backend) PutObject(path string, content []byte) error {
	return b.PutObjectStream(path, bytes.NewBuffer(content))
}

// DeleteObject removes an object from Amazon S3 bucket, at prefix
func (b AmazonS3Backend) DeleteObject(path string) error {
	s3Input := &s3.DeleteObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(cleanPrefix(pathutil.Join(b.Prefix, path))),
	}
	_, err := b.Client.DeleteObject(s3Input)
	return err
}

// GetObjectStream retrieves an object stream from Amazon S3 bucket, at prefix
func (b AmazonS3Backend) GetObjectStream(path string) (*ObjectStream, error) {
	object := &ObjectStream{}
	object.Path = path
	s3Input := &s3.GetObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(cleanPrefix(pathutil.Join(b.Prefix, path))),
	}
	s3Result, err := b.Client.GetObject(s3Input)
	if err != nil {
		return object, err
	}
	object.Content = s3Result.Body
	object.LastModified = *s3Result.LastModified
	return object, nil
}

// PutObject uploads an object stream to Amazon S3 bucket, at prefix
func (b AmazonS3Backend) PutObjectStream(path string, content io.Reader) error {
	var ct string
	if seeker, ok := content.(io.ReadSeeker); ok {
		buff := make([]byte, 512)
		_, err := seeker.Seek(0, io.SeekStart)
		if err == nil {
			n, err := seeker.Read(buff)
			if err == nil || errors.Is(err, io.EOF) {
				ct = http.DetectContentType(buff[:n])
			}
			// try to seek to the beginning of the file
			seeker.Seek(0, io.SeekStart)
		}
	}

	s3Input := &s3manager.UploadInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(cleanPrefix(pathutil.Join(b.Prefix, path))),
		Body:   content,
	}

	if ct != "" {
		s3Input.ContentType = aws.String(ct)
	}

	if b.SSE != "" {
		s3Input.ServerSideEncryption = aws.String(b.SSE)
	}

	_, err := b.Uploader.Upload(s3Input)
	return err
}

func (b AmazonS3Backend) PutObjectStreamWithACL(path string, content io.Reader, ACL *string) error {
	var ct string
	if seeker, ok := content.(io.ReadSeeker); ok {
		buff := make([]byte, 512)
		_, err := seeker.Seek(0, io.SeekStart)
		if err == nil {
			n, err := seeker.Read(buff)
			if err == nil || errors.Is(err, io.EOF) {
				ct = http.DetectContentType(buff[:n])
			}
			// try to seek to the beginning of the file
			seeker.Seek(0, io.SeekStart)
		}
	}

	s3Input := &s3manager.UploadInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(cleanPrefix(pathutil.Join(b.Prefix, path))),
		Body:   content,
		ACL:    ACL,
	}

	if ct != "" {
		s3Input.ContentType = aws.String(ct)
	}

	if b.SSE != "" {
		s3Input.ServerSideEncryption = aws.String(b.SSE)
	}

	_, err := b.Uploader.Upload(s3Input)
	return err
}

func (b AmazonS3Backend) HandleHttpFileDownload(w http.ResponseWriter, r *http.Request, path string) {
	// https://github.com/oxyno-zeta/s3-proxy/blob/08de1e6c9b694134912ad0fcd17461d1225b39fe/pkg/s3-proxy/server/server.go#L238

	// Get If-Modified-Since as string
	ifModifiedSinceStr := r.Header.Get("If-Modified-Since")
	// Create result
	var ifModifiedSince *time.Time
	// Check if content exists
	if ifModifiedSinceStr != "" {
		// Parse time
		ifModifiedSinceTime, err := http.ParseTime(ifModifiedSinceStr)
		// Check error
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Save result
		ifModifiedSince = &ifModifiedSinceTime
	}

	// Get If-Match
	ifMatch := r.Header.Get("If-Match")

	// Get If-None-Match
	ifNoneMatch := r.Header.Get("If-None-Match")

	// Get If-Unmodified-Since as string
	ifUnmodifiedSinceStr := r.Header.Get("If-Unmodified-Since")
	// Create result
	var ifUnmodifiedSince *time.Time
	// Check if content exists
	if ifUnmodifiedSinceStr != "" {
		// Parse time
		ifUnmodifiedSinceTime, err := http.ParseTime(ifUnmodifiedSinceStr)
		// Check error
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Save result
		ifUnmodifiedSince = &ifUnmodifiedSinceTime
	}

	s3Input := &s3.GetObjectInput{
		Bucket:            aws.String(b.Bucket),
		Key:               aws.String(cleanPrefix(pathutil.Join(b.Prefix, path))),
		IfModifiedSince:   ifModifiedSince,
		IfUnmodifiedSince: ifUnmodifiedSince,
	}

	// https://github.com/oxyno-zeta/s3-proxy/blob/08de1e6c9b694134912ad0fcd17461d1225b39fe/pkg/s3-proxy/s3client/s3Context.go#L161

	// Add If Match if not empty
	if ifMatch != "" {
		s3Input.IfMatch = aws.String(ifMatch)
	}

	// Add If None Match if not empty
	if ifNoneMatch != "" {
		s3Input.IfNoneMatch = aws.String(ifNoneMatch)
	}

	s3Result, err := b.Client.GetObject(s3Input)
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				w.WriteHeader(http.StatusNotFound)
			case "NotModified":
				w.WriteHeader(http.StatusNotModified)
			case "PreconditionFailed":
				w.WriteHeader(http.StatusPreconditionFailed)
			default:
				w.WriteHeader(http.StatusBadRequest)
			}
			return
		}

		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer s3Result.Body.Close()

	// https://github.com/oxyno-zeta/s3-proxy/blob/08de1e6c9b694134912ad0fcd17461d1225b39fe/pkg/s3-proxy/bucket/utils.go#L15
	// set headers
	httpStatus := http.StatusOK
	if s3Result.CacheControl != nil {
		w.Header().Add("Cache-Control", *s3Result.CacheControl)
	}

	if s3Result.Expires != nil {
		w.Header().Add("Expires", *s3Result.Expires)
	}

	if s3Result.ContentDisposition != nil {
		w.Header().Add("Content-Disposition", *s3Result.ContentDisposition)
	}

	if s3Result.ContentEncoding != nil {
		w.Header().Add("Content-Encoding", *s3Result.ContentEncoding)
	}

	if s3Result.ContentLanguage != nil {
		w.Header().Add("Content-Language", *s3Result.ContentLanguage)
	}

	if s3Result.ContentLength != nil {
		w.Header().Add("Content-Length", strconv.FormatInt(*s3Result.ContentLength, 10))
	}

	if s3Result.ContentRange != nil {
		w.Header().Add("Content-Range", *s3Result.ContentRange)

		// https://github.com/oxyno-zeta/s3-proxy/blob/08de1e6c9b694134912ad0fcd17461d1225b39fe/pkg/s3-proxy/bucket/utils.go#L31
		if len(*s3Result.ContentRange) > 0 && s3Result.ContentLength != nil {
			//https://github.com/oxyno-zeta/s3-proxy/blob/08de1e6c9b694134912ad0fcd17461d1225b39fe/pkg/s3-proxy/bucket/utils.go#L62
			s := strings.Split(*s3Result.ContentRange, "/")
			if len(s) > 1 {
				totalSizeString := s[1]
				totalSizeString = strings.TrimSpace(totalSizeString)

				totalSize, err := strconv.ParseInt(totalSizeString, 10, 64)
				if err == nil && totalSize != *s3Result.ContentLength {
					httpStatus = http.StatusPartialContent
				}
			}
		}
	}

	if s3Result.ContentType != nil {
		w.Header().Add("Content-Type", *s3Result.ContentType)
	}

	if s3Result.ETag != nil {
		w.Header().Add("ETag", *s3Result.ETag)
	}

	if s3Result.LastModified != nil {
		w.Header().Add("Last-Modified", s3Result.LastModified.UTC().Format(http.TimeFormat))
	}

	if r.Method != http.MethodHead {
		_, err = io.Copy(w, s3Result.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(httpStatus)
}

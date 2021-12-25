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
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	pathutil "path"
	"time"

	"github.com/tencentyun/cos-go-sdk-v5"
)

// TencentCloudCOSBackend is a storage backend for Tencent Cloud COS
type TencentCloudCOSBackend struct {
	Bucket *cos.BucketService
	Object *cos.ObjectService
	Client *cos.Client
	Prefix string
}

const (
	HTTPHeaderLastModified = "Last-Modified"
)

// NewTencentCloudCOSBackend creates a new instance of TencentCloudCOSBackend
func NewTencentCloudCOSBackend(bucket string, prefix string, endpoint string) *TencentCloudCOSBackend {

	secretID := os.Getenv("TENCENT_CLOUD_COS_SECRET_ID")
	secretKey := os.Getenv("TENCENT_CLOUD_COS_SECRET_KEY")

	if len(secretID) == 0 {
		panic("TENCENT_CLOUD_COS_SECRET_ID environment variable is not set")
	}

	if len(secretKey) == 0 {
		panic("TENCENT_CLOUD_COS_SECRET_KEY environment variable is not set")
	}

	if len(endpoint) == 0 {
		// Set default endpoint
		endpoint = "cos.ap-guangzhou.myqcloud.com"
	}

	bucketURL, err := url.Parse("http://" + bucket + "." + endpoint)
	if err != nil {
		panic("Access domain is error: http://" + bucket + "." + endpoint)
	}
	baseURL := &cos.BaseURL{BucketURL: bucketURL}

	client := cos.NewClient(baseURL, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  secretID,
			SecretKey: secretKey,
		},
	})

	tencentCloudCOSBackend := &TencentCloudCOSBackend{
		Bucket: client.Bucket,
		Object: client.Object,
		Client: client,
		Prefix: cleanPrefix(prefix),
	}
	return tencentCloudCOSBackend
}

// ListObjects lists all objects in Tencent Cloud COS bucket, at prefix
func (t TencentCloudCOSBackend) ListObjects(prefix string) ([]Object, error) {

	var objects []Object

	prefix = pathutil.Join(t.Prefix, prefix)
	cosPrefix := prefix
	cosMarker := ""

	for {
		opt := &cos.BucketGetOptions{
			Prefix:  cosPrefix,
			MaxKeys: 100,
			Marker:  cosMarker,
		}
		bucketGetResult, _, err := t.Bucket.Get(context.Background(), opt)
		if err != nil {
			return objects, err
		}

		for _, obj := range bucketGetResult.Contents {
			path := removePrefixFromObjectPath(prefix, obj.Key)
			if objectPathIsInvalid(path) {
				continue
			}
			lastModified, _ := time.Parse(time.RFC3339, obj.LastModified)
			object := Object{
				Metadata: Metadata{
					Path:         path,
					LastModified: lastModified,
				},
				Content: []byte{},
			}
			objects = append(objects, object)
		}

		if !bucketGetResult.IsTruncated {
			break
		}

		cosPrefix = bucketGetResult.Prefix
		cosMarker = bucketGetResult.NextMarker
	}

	return objects, nil
}

// ListObjectsFromDirectory lists all objects under prefix, always with depth 1, returning at most limit objects (directories + files)
// It's intent is to abstract a directory listing
// Make sure prefix is a full path, other cases might give unexpected results
// If limit <= 0, it will return at most all the objects in 'prefix', limiting only by the backend limits
// You can know if the response is complete calling output.IsTruncated(), if true then the response isn't complete
func (b TencentCloudCOSBackend) ListObjectsFromDirectory(prefix string, limit int) (ListObjectsFromDirectoryOutput, error) {
	// TODO
	return nil, ErrNotImplemented
}

func (b TencentCloudCOSBackend) RenamePrefixOrObject(path, newPath string) error {
	// TODO
	return ErrNotImplemented
}

// GetObject retrieves an object from Tencent Cloud COS bucket, at prefix
func (t TencentCloudCOSBackend) GetObject(path string) (Object, error) {

	var object Object
	object.Path = path

	var content []byte
	key := pathutil.Join(t.Prefix, path)

	opt := &cos.ObjectGetOptions{}
	resp, err := t.Object.Get(context.Background(), key, opt)
	if err != nil {
		return object, err
	}

	content, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return object, err
	}
	object.Content = content

	lastModified, err := http.ParseTime(resp.Header.Get(HTTPHeaderLastModified))
	if err != nil {
		return object, err
	}

	object.LastModified = lastModified
	return object, nil
}

// PutObject uploads an object to Tencent Cloud COS bucket, at prefix
func (t TencentCloudCOSBackend) PutObject(path string, content []byte) error {

	key := pathutil.Join(t.Prefix, path)
	var err error

	opt := &cos.ObjectPutOptions{}
	_, err = t.Object.Put(context.Background(), key, bytes.NewReader(content), opt)

	return err
}

// DeleteObject removes an object from Tencent Cloud COS bucket, at prefix
func (t TencentCloudCOSBackend) DeleteObject(path string) error {

	key := pathutil.Join(t.Prefix, path)
	_, err := t.Object.Delete(context.Background(), key)
	return err
}

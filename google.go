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
	"io/ioutil"
	pathutil "path"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

// GoogleCSBackend is a storage backend for Google Cloud Storage
type GoogleCSBackend struct {
	Prefix  string
	Client  *storage.BucketHandle
	Context context.Context
}

// NewGoogleCSBackend creates a new instance of GoogleCSBackend
func NewGoogleCSBackend(bucket string, prefix string) *GoogleCSBackend {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}
	bucketHandle := client.Bucket(bucket)
	prefix = cleanPrefix(prefix)
	b := &GoogleCSBackend{
		Prefix:  prefix,
		Client:  bucketHandle,
		Context: ctx,
	}
	return b
}

// ListObjects lists all objects in Google Cloud Storage bucket, at prefix
func (b GoogleCSBackend) ListObjects(prefix string) ([]Object, error) {
	var objects []Object
	prefix = pathutil.Join(b.Prefix, prefix)
	listQuery := &storage.Query{
		Prefix: prefix,
	}
	it := b.Client.Objects(b.Context, listQuery)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return objects, err
		}
		path := removePrefixFromObjectPath(prefix, attrs.Name)
		if objectPathIsInvalid(path) {
			continue
		}
		object := Object{
			Metadata: Metadata{
				Path:         path,
				LastModified: attrs.Updated,
			},
			Content: []byte{},
		}
		objects = append(objects, object)
	}
	return objects, nil
}

// ListObjectsFromDirectory lists all objects under prefix, always with depth 1, returning at most limit objects (directories + files)
// It's intent is to abstract a directory listing
// Make sure prefix is a full path, other cases might give unexpected results
// If limit <= 0, it will return at most all the objects in 'prefix', limiting only by the backend limits
// You can know if the response is complete calling output.IsTruncated(), if true then the response isn't complete
func (b GoogleCSBackend) ListObjectsFromDirectory(prefix string, limit int) (ListObjectsFromDirectoryOutput, error) {
	// TODO
	return nil, ErrNotImplemented
}

func (b GoogleCSBackend) RenamePrefixOrObject(path, newPath string) error {
	// TODO
	return ErrNotImplemented
}

// GetObject retrieves an object from Google Cloud Storage bucket, at prefix
func (b GoogleCSBackend) GetObject(path string) (Object, error) {
	var object Object
	object.Path = path
	objectHandle := b.Client.Object(pathutil.Join(b.Prefix, path))
	attrs, err := objectHandle.Attrs(b.Context)
	if err != nil {
		return object, err
	}
	object.LastModified = attrs.Updated
	rc, err := objectHandle.NewReader(b.Context)
	if err != nil {
		return object, err
	}
	content, err := ioutil.ReadAll(rc)
	rc.Close()
	if err != nil {
		return object, err
	}
	object.Content = content
	return object, nil
}

// PutObject uploads an object to Google Cloud Storage bucket, at prefix
func (b GoogleCSBackend) PutObject(path string, content []byte) error {
	wc := b.Client.Object(pathutil.Join(b.Prefix, path)).NewWriter(b.Context)
	_, err := wc.Write(content)
	if err != nil {
		return err
	}
	err = wc.Close()
	return err
}

// DeleteObject removes an object from Google Cloud Storage bucket, at prefix
func (b GoogleCSBackend) DeleteObject(path string) error {
	err := b.Client.Object(pathutil.Join(b.Prefix, path)).Delete(b.Context)
	return err
}

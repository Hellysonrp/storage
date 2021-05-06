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
	"io"
	"io/ioutil"
	"net/http"
	"os"

	pathutil "path"
	"path/filepath"
)

// LocalFilesystemBackend is a storage backend for local filesystem storage
type LocalFilesystemBackend struct {
	RootDirectory string
}

// NewLocalFilesystemBackend creates a new instance of LocalFilesystemBackend
func NewLocalFilesystemBackend(rootDirectory string) *LocalFilesystemBackend {
	absPath, err := filepath.Abs(rootDirectory)
	if err != nil {
		panic(err)
	}
	b := &LocalFilesystemBackend{RootDirectory: absPath}
	return b
}

// ListObjects lists all objects in root directory (depth 1)
func (b LocalFilesystemBackend) ListObjects(prefix string) ([]Object, error) {
	var objects []Object
	files, err := ioutil.ReadDir(pathutil.Join(b.RootDirectory, prefix))
	if err != nil {
		if os.IsNotExist(err) { // OK if the directory doesnt exist yet
			err = nil
		}
		return objects, err
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		object := Object{
			Metadata: Metadata{
				Path:         f.Name(),
				LastModified: f.ModTime(),
			},
			Content: []byte{},
		}
		objects = append(objects, object)
	}
	return objects, nil
}

// GetObject retrieves an object from root directory
func (b LocalFilesystemBackend) GetObject(path string) (Object, error) {
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

// PutObject puts an object in root directory
func (b LocalFilesystemBackend) PutObject(path string, content []byte) error {
	return b.PutObjectStream(path, bytes.NewBuffer(content))
}

// DeleteObject removes an object from root directory
func (b LocalFilesystemBackend) DeleteObject(path string) error {
	fullpath := pathutil.Join(b.RootDirectory, path)
	err := os.Remove(fullpath)
	return err
}

// GetObjectStream retrieves an object stream from root directory
func (b LocalFilesystemBackend) GetObjectStream(path string) (*ObjectStream, error) {
	object := &ObjectStream{}
	object.Path = path
	fullpath := pathutil.Join(b.RootDirectory, path)
	content, err := os.Open(fullpath)
	if err != nil {
		return object, err
	}
	object.Content = content
	info, err := content.Stat()
	if err != nil {
		return object, err
	}
	object.LastModified = info.ModTime()
	return object, err
}

// PutObjectStream puts an object stream in root directory
func (b LocalFilesystemBackend) PutObjectStream(path string, content io.Reader) error {
	fullpath := pathutil.Join(b.RootDirectory, path)
	folderPath := pathutil.Dir(fullpath)
	_, err := os.Stat(folderPath)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(folderPath, 0777)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	fp, err := os.Create(fullpath)
	if err != nil {
		return err
	}

	// https://stackoverflow.com/a/9739903/6762842
	buf := make([]byte, 4096)
	for {
		// read a chunk
		n, err := content.Read(buf)
		if err != nil && err != io.EOF {
			fp.Close()
			return err
		}
		if n == 0 {
			break
		}

		// write a chunk
		if _, err := fp.Write(buf[:n]); err != nil {
			fp.Close()
			return err
		}
	}

	return fp.Close()
}

func (b LocalFilesystemBackend) HandleHttpFileDownload(w http.ResponseWriter, r *http.Request, path string) {
	obj, err := b.GetObjectStream(path)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
		}
		w.WriteHeader(http.StatusInternalServerError)
	}

	rs := obj.Content.(io.ReadSeeker)
	http.ServeContent(w, r, obj.Name, obj.LastModified, rs)
}

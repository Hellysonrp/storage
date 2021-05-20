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
	"os"
	"strings"

	pathutil "path"
	"path/filepath"
)

type localListObjectsFromDirectoryOutput struct {
	prefix          string
	directory       *os.File
	limit           int
	filesRead       []Metadata
	directoriesRead []Metadata
	nextPageCalled  bool
	isEOF           bool
}

func (l *localListObjectsFromDirectoryOutput) GetDirectories() []Metadata {
	return l.directoriesRead
}

func (l *localListObjectsFromDirectoryOutput) GetFiles() []Metadata {
	return l.filesRead
}

func (l *localListObjectsFromDirectoryOutput) IsTruncated() bool {
	return !l.isEOF
}

func (l *localListObjectsFromDirectoryOutput) NextPage() (ListObjectsFromDirectoryOutput, error) {
	if l.nextPageCalled {
		return nil, errors.New("you cannot call NextPage more than once")
	}

	r := &localListObjectsFromDirectoryOutput{
		directory: l.directory,
		prefix:    l.prefix,
		limit:     l.limit,
	}

	if l.isEOF {
		r.isEOF = true
		return r, io.EOF
	}

	r.directoriesRead = make([]Metadata, 0, 5)
	r.filesRead = make([]Metadata, 0, 5)

	entries, err := l.directory.ReadDir(l.limit)

	if len(entries) > 0 {
		for _, e := range entries {
			m := Metadata{
				Path: pathutil.Join(l.prefix, e.Name()),
			}
			if e.IsDir() {
				r.directoriesRead = append(r.directoriesRead, m)
			} else {
				r.filesRead = append(r.filesRead, m)
			}
		}
	}

	r.isEOF = (l.limit > 0 && err != nil && errors.Is(err, io.EOF)) || (l.limit <= 0 && err == nil)

	if r.isEOF && err == nil {
		// always returns io.EOF if EOF
		err = io.EOF
	}

	l.nextPageCalled = true

	return r, err
}

func (l *localListObjectsFromDirectoryOutput) FreeFromMemory() {
	l.directoriesRead = nil
	l.filesRead = nil
}

func (l *localListObjectsFromDirectoryOutput) Close() {
	l.FreeFromMemory()
	l.directory.Close()
}

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

// ListObjectsFromDirectory lists all objects under prefix, always with depth 1, returning at most limit objects (directories + files)
// It's intent is to abstract a directory listing
// Make sure prefix is a full path, other cases might give unexpected results
// If limit <= 0, it will return at most all the objects in 'prefix', limiting only by the backend limits
// You can know if the response is complete calling output.IsTruncated(), if true then the response isn't complete
func (b LocalFilesystemBackend) ListObjectsFromDirectory(prefix string, limit int) (ListObjectsFromDirectoryOutput, error) {
	output := &localListObjectsFromDirectoryOutput{
		prefix: prefix,
		limit:  limit,
	}

	fullPath := pathutil.Join(b.RootDirectory, prefix)
	f, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			output.isEOF = true
			return output, nil
		}
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, errors.New("prefix is not a directory")
	}

	output.directory = f

	return output.NextPage()
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
	info, err := content.Stat()
	if err != nil {
		return object, err
	}
	if info.IsDir() {
		return object, errors.New("path must lead to a file, found directory")
	}
	object.Content = content
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
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	lastDirSeparatorIndex := strings.LastIndex(obj.Path, "/")
	name := obj.Path[lastDirSeparatorIndex+1:]

	rs := obj.Content.(io.ReadSeeker)
	http.ServeContent(w, r, name, obj.LastModified, rs)

	obj.Content.Close()
}

package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

// CASPathTransformFunc is Content Addressable path transform
// function
func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	fmt.Printf("hashStr %s\n", hashStr)
	blocksize := 5
	sliceLen := len(hashStr) / blocksize

	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i * blocksize, ( i *  blocksize) + blocksize
		paths[i] = hashStr[from:to]
	}
	return PathKey {
		PathName: strings.Join(paths, "/"),
		FileName: hashStr,
	}
}

type PathTransformFunc func (string) PathKey

const defaultRootFolderName = "dfsgonetwork"

func DefaultPathTransformFunc( key string) PathKey {
	return PathKey{
		PathName: key,
		FileName: key,
	}
}

type PathKey struct {
	PathName string
	FileName string
}

func(p PathKey) FirstPathName() string {
	paths := strings.Split(p.PathName, "/")

	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.FileName)
}

type StoreOpts struct{
	// Root is the name of root folder, containing all the folders/files
	// of the system
	Root 			  string
	PathTransformFunc PathTransformFunc
}

type Store struct{
	StoreOpts
}

func NewStore(opts StoreOpts) *Store {
	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = DefaultPathTransformFunc
	}
	if len(opts.Root) == 0 {
		opts.Root = defaultRootFolderName
	}
	return &Store{
		StoreOpts: opts,
	}
}

func (s *Store) Read(key string) (io.Reader, error) {
	f, err := s.readStream(key)

	if err != nil {
		return nil, err
	}
	defer f.Close()
	
	
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, f)

	return buf, err

}

func (s *Store) readStream(key string) (io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())
	return os.Open(fullPathWithRoot)
}

func (s *Store) Write(key string, r io.Reader) (int64, error) {
	return s.writeStream(key, r)
}

func (s *Store) writeStream(key string, r io.Reader) (int64, error) {
	pathKey := s.PathTransformFunc(key)
	pathNameWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.PathName)
	if err := os.MkdirAll( pathNameWithRoot, os.ModePerm); err != nil {
		return 0, err
	}

	fullPathWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())
	f, err := os.Create(fullPathWithRoot)
	
	if err != nil {
		return 0, err
	}

	defer f.Close()

	n, err :=  io.Copy(f, r)
	if err != nil {
		return 0, err
	}

	log.Printf("written %d bytes to disk: %s", n, fullPathWithRoot)

	return n, nil
}

func (s *Store) Has(key string) bool  {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())
	
	if _, err := os.Stat(fullPathWithRoot); err == nil {
		return true
	}

	return false
}

func (s *Store) Delete(key string) error {
	pathKey := s.PathTransformFunc(key)

	defer func ()  {
		log.Printf("deleted [%s] from disk", pathKey.FileName)
	}()

	firstPathNameWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.FirstPathName())

	return os.RemoveAll(firstPathNameWithRoot)
}

func (s *Store) Clear() error {
	return os.RemoveAll((s.Root))
}

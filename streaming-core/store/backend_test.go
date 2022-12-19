package store

import (
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"os"
	"testing"
)

func tempFSBackend(checkpointsNumRetained int) (Backend, error) {
	if mkdirTemp, err := os.MkdirTemp("", ""); err != nil {
		return nil, err
	} else {
		return NewFSBackend(zap.L(), mkdirTemp, checkpointsNumRetained)
	}
}

func TestFSBackendSaveAndGet(t *testing.T) {
	fsBackend, err := tempFSBackend(1)
	assert.Nil(t, err)
	err = fsBackend.Save(1, "tt", []byte{123, 123, 123})
	assert.Nil(t, err)
	get, err := fsBackend.Get("tt")
	assert.Nil(t, err)
	assert.Nil(t, get)
	assert.Nil(t, fsBackend.Persist(1))
	get, err = fsBackend.Get("tt")
	assert.Nil(t, err)
	assert.Equal(t, get, []byte{123, 123, 123})
}

func TestIteratorBucket(t *testing.T) {
	backend, _ := NewFSBackend(zap.L(), "/Users/klein/GoLandProjects/athena/tmp", 1)
	_ = backend.Close()
}

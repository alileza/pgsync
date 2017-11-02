package syncmap

import (
	"encoding/json"
	"sync"
)

type SyncMap struct {
	sync.RWMutex
	s map[string]interface{}
}

func New() *SyncMap {
	return &SyncMap{
		s: make(map[string]interface{}),
	}
}

func (s *SyncMap) Store(key string, val interface{}) {
	s.Lock()
	defer s.Unlock()

	s.s[key] = val
}

func (s *SyncMap) Load(key string) interface{} {
	s.RLock()
	defer s.RUnlock()
	ret := s.s[key]
	if ret == nil {
		return 0
	}
	return ret
}

func (s *SyncMap) LoadFromByte(b []byte) error {
	return json.Unmarshal(b, &s.s)
}

func (s *SyncMap) ToByte() ([]byte, error) {
	return json.Marshal(s.s)
}

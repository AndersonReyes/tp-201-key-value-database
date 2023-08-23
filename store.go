package main

type Store struct {
	data map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

func (s *Store) Get(key string) (string, bool) {
	val, ok := s.data[key]
	return val, ok
}

func (s *Store) Set(key string, value string) error {
	s.data[key] = value

	return nil
}

func (s *Store) Remove(key string) error {
	delete(s.data, key)
	return nil
}

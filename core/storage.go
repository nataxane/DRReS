package core

type Storage struct {
	tables map[string](Table)
}

func InitStorage() (storage Storage) {
	storage = Storage{map[string]Table{"default": {}}}
	return
}

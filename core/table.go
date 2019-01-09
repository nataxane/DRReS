package core

import (
	"fmt"
)

type Record string

type Table map[string]Record

func (t Table) Get(key string) (value Record, err error) {
	value, ok := t[key]
	if ok == false {
		return "", fmt.Errorf("key %v is not in the table", key)
	} else {
		return value, nil
	}
}

func (t Table) Put(key string, value Record) (err error) {
	_, ok := t[key]
	if ok == true {
		return fmt.Errorf("key %v is already in the table", key)
	} else {
		t[key] = value
		return
	}
}

func (t Table) Delete(key string) (err error) {
	_, ok := t[key]
	if ok == false {
		return fmt.Errorf("key %v is not in the table", key)
	} else {
		delete(t, key)
		return nil
	}
}

func main() {
	var table = Table{}

	var natalia, err = table.Get("7")
	if err == nil {
		fmt.Println(natalia)
	} else {
		fmt.Println(err)
	}

	fmt.Println(table)

}
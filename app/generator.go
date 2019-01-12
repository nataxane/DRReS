package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"
)

func connectServer() net.Conn {
	addr := net.JoinHostPort("localhost", "8080")
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Connected to port 8080")

	buff := make([]byte, 1024)
	n, _ := conn.Read(buff)
	fmt.Printf("%s\n", buff[:n])

	return conn
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomStringGenerator() string {
	n := 3 + rand.Intn(20)
	b := make([]byte, n)

	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

func generateWorkload(conn net.Conn) {
	var (
		randInt int
		op, value string
		key int
		query string
		keys = make([]int, 0, 1000)
	)

	defer conn.Close()

	for i := 0; i < 100000; i++ {
		if len(keys) == 0 {
			randInt = rand.Intn(40)
		} else {
			randInt = rand.Intn(100)
		}

		switch {
		case randInt < 45:
			op = "insert"
			key = rand.Intn(1000000)
			value = randomStringGenerator()
			keys = append(keys, key)
		case randInt < 55:
			op = "read"
			keyIdx := rand.Intn(len(keys))
			key = keys[keyIdx]
		case randInt < 80:
			op = "update"
			keyIdx := rand.Intn(len(keys))
			key = keys[keyIdx]
			value = randomStringGenerator()
		case randInt < 100:
			op = "delete"
			keyIdx := rand.Intn(len(keys))
			key = keys[keyIdx]
			keys = append(keys[:keyIdx], keys[keyIdx+1:]...)
		}

		switch {
		case op == "insert" || op == "update":
			query = fmt.Sprintf("%s %d %s", op, key, value)
		default:
			query = fmt.Sprintf("%s %d", op, key)
		}
		fmt.Println(query)
		conn.Write([]byte(query))

		buff := make([]byte, 1024)
		n, _ := conn.Read(buff)
		fmt.Printf("%s\n", buff[:n])
	}
}

func main() {
	conn := connectServer()
	time.Sleep(20)
	generateWorkload(conn)
}
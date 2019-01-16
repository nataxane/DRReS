package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
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

func generateWorkload(conn net.Conn, queryNum *int) {
	var (
		randInt int
		op, value string
		key int
		query string
		keys = make([]int, 0, 1000)
	)

	defer conn.Close()

	for i := 0; i < *queryNum; i++ {
		if len(keys) == 0 {
			randInt = rand.Intn(40)
		} else {
			randInt = rand.Intn(100)
		}

		switch {
		case randInt < 45:
			op = "insert"
			key = rand.Intn(*queryNum*10)
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
		conn.Write([]byte(query))

		buff := make([]byte, 1024)
		conn.Read(buff)
	}
}

func main() {
	conn := connectServer()

	size := flag.Int("size", 1000, "workload size")
	flag.Parse()

	generateWorkload(conn, size)
}
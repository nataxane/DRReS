package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

func connectServer(host *string, port *string) net.Conn {
	addr := net.JoinHostPort(*host, *port)
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Connected to port %s\n", *port)

	buff := make([]byte, 1024)
	conn.Read(buff)

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

func generateWorkload(conn net.Conn, queryNum *int, pool *sync.WaitGroup) {
	var (
		randInt int
		op, value string
		key int
		query string
		deletedKeys []int
	)

	defer conn.Close()

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < *queryNum; i++ {
		if len(deletedKeys) == 0 {
			randInt = rand.Intn(90)
		} else {
			randInt = rand.Intn(100)
		}

		switch {
		case randInt < 80:
			op = "update"
			key = rand.Intn(4000000)
			value = randomStringGenerator()
		case randInt < 100:
			op = "read"
			key = rand.Intn(4000000)
		//case randInt < 90:
		//	op = "delete"
		//	key := rand.Intn(4010000)
		//	deletedKeys = append(deletedKeys, key)
		//case randInt < 100:
		//	op = "insert"
		//	keyIdx := rand.Intn(len(deletedKeys))
		//	key = deletedKeys[keyIdx]
		//	value = randomStringGenerator()
		//	deletedKeys = append(deletedKeys[:keyIdx], deletedKeys[keyIdx+1:]...)
		}

		switch {
		case op == "insert" || op == "update":
			query = fmt.Sprintf("%s %d %s", op, key, value)
		default:
			query = fmt.Sprintf("%s %d", op, key)
		}
		_, err := conn.Write([]byte(query))
		if err != nil {
			log.Println(err)
			return
		}

		buff := make([]byte, 1024)
		_, err = conn.Read(buff)
		if err != nil {
			log.Println(err)
			return
		}
	}

	pool.Done()
}

func runClient(host *string, port *string, queryNum *int, pool *sync.WaitGroup) {
	conn := connectServer(host, port)
	generateWorkload(conn, queryNum, pool)
}

func main() {
	host := flag.String("host", "localhost", "host")
	port := flag.String("port", "8080", "port")
	size := flag.Int("size", 1000, "workload size")
	threads := flag.Int("clients", 1, "number of clients")
	flag.Parse()

	pool := sync.WaitGroup{}

	for i := 0; i < *threads; i++  {
		pool.Add(1)
		go runClient(host, port, size, &pool)
	}

	pool.Wait()
}
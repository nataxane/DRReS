package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"storage/core"
)

var clientId = 0

var welcome = []byte("Welcome to DRReS!\n\n" +
	"Here what you can do here:\n" +
	"\tget <key> – read a record\n" +
	"\tput <key> <value> – insert a new record\n" +
	"\tdelete <key> – delete a record\n" +
	"\tshow – show current table snapshot\n\n" +
	"Please have fun and don't forget to crash.\n")

func handler(conn net.Conn, storage core.Storage, clientId int) {
	defer func() {
		log.Printf("Client %d disconnected", clientId)
		conn.Close()
	}()

	var (
		buf = make([]byte, 1024)
		reader = bufio.NewReader(conn)
		writer = bufio.NewWriter(conn)
	)

	writer.Write(welcome)
	writer.Flush()

OUTLOOP:
	for {
		n, err := reader.Read(buf)

		switch err {
		case io.EOF:
			break OUTLOOP
		case nil:
			query := string(buf[:n])
			log.Printf("Recieved query: %s", query)

			reply := core.ProcessQuery(query, storage)

			writer.Write(reply)
			writer.Flush()
		default:
			log.Printf("Recieving data failed: %s\n", err)
		}
	}
}

func SocketServer(port string) {
	listen, err := net.Listen("tcp", ":"+port)
	defer listen.Close()

	if err != nil {
		log.Fatalf("Socket listen port %s failed, %s", port, err)
	}

	storage := core.InitStorage()
	log.Printf("Begin listen to port: %s", port)

	for {
		conn, err := listen.Accept()

		if err != nil {
			log.Println(err)
			continue
		}

		log.Printf("Connected client %d", clientId)

		go handler(conn, storage, clientId)

		clientId += 1
	}
}

func main() {
	port := "8080"

	SocketServer(port)
}

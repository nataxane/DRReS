package main

import (
	"bufio"
	"github.com/DRReS/core"
	"io"
	"log"
	"net"
)

var clientId = 0

var welcome = []byte("Welcome to DRReS!\n\n" +
	"Here what you can do here:\n" +
	"\tread <key> – read a record\n" +
	"\tinsert <key> <value> – insert a new record\n" +
	"\tupdate <key> <value> – update an existing record\n" +
	"\tdelete <key> – delete a record\n\n" +
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
			log.Printf("Recieved query from %d: %s", clientId, query)

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

	if err != nil {
		log.Fatalf("Socket listen port %s failed, %s", port, err)
	}

	log.Printf("Begin listen to port: %s", port)

	storage := core.InitStorage()
	checkpointScheduler := storage.RunCheckpointing()
	statsScheduler, metric:= core.RunStats(storage)

	defer func() {
		listen.Close()
		checkpointScheduler.Stop()
		statsScheduler.Stop()
		metric.ShowPlot()
	}()

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

	log.SetFlags(log.Lmicroseconds)

	SocketServer(port)
}

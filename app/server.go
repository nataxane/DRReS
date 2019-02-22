package main

import (
	"bufio"
	"github.com/DRReS/core"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)


var welcome = []byte("Welcome to DRReS!\n\n" +
	"Here what you can do here:\n" +
	"\tread <key> – read a record\n" +
	"\tinsert <key> <value> – insert a new record\n" +
	"\tupdate <key> <value> – update an existing record\n" +
	"\tdelete <key> – delete a record\n\n" +
	"Please have fun and don't forget to crash.\n")

var clientId = 0

func handler(clientId int, conn net.Conn, storage core.Storage, stopChan chan struct{}, clientPool *sync.WaitGroup) {
	defer func() {
		conn.Close()
		clientPool.Done()
		log.Printf("Client %d disconnected", clientId)
	}()

	var (
		buf = make([]byte, 1024)
		reader = bufio.NewReader(conn)
		writer = bufio.NewWriter(conn)
	)

	writer.Write(welcome)
	writer.Flush()

	for {
		select {
		case <- stopChan:
			return
		default:
			conn.SetReadDeadline(time.Now().Add(1e9))
			n, err := reader.Read(buf)

			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}

			if err == io.EOF {
				return
			}

			if err != nil {
				log.Printf("Recieving data failed: %s\n", err)
				continue
			}

			query := string(buf[:n])
			log.Printf("Recieved query from %d: %s", clientId, query)

			reply := core.ProcessQuery(query, storage)

			writer.Write(reply)
			writer.Flush()
		}
	}
}

func SocketServer() {
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:8080")
	listener, err := net.ListenTCP("tcp", addr)

	if err != nil {
		log.Fatalf("Socket listen port 8080 failed, %s", err)
	}

	log.Println("Begin listen to port: 8080")

	storage := core.InitStorage()
	statsScheduler := core.RunMetrics(storage)

	defer func() {
		listener.Close()

		statsScheduler.Stop()
		storage.Stop()

		core.SaveMetrics(storage)
	}()

	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, os.Interrupt, os.Kill, syscall.SIGTERM)

	stopHandlerChan := make(chan struct{})

	clientPool := sync.WaitGroup{}

	for {
		select {
		case <-quitChan:
			log.Println("Shutting down...")
			close(stopHandlerChan)
			clientPool.Wait()
			return
		default:
			listener.SetDeadline(time.Now().Add(1e9))

			conn, err := listener.Accept()

			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}

			if err != nil {
				log.Printf("Can not establish connection to a new client: %s", err)
				continue
			}

			log.Printf("Connected client %d", clientId)

			clientPool.Add(1)
			go handler(clientId, conn, storage, stopHandlerChan, &clientPool)
			clientId += 1
		}
	}
}

func main() {
	log.SetFlags(log.Lmicroseconds)
	SocketServer()
}

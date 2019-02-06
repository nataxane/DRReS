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


type clientPool struct {
	currentId int
	wg sync.WaitGroup
	active map[int]bool
}

func (pool *clientPool) connect() int {
	clientId := pool.currentId

	pool.wg.Add(1)
	pool.active[clientId] = true

	log.Printf("Connected client %d", clientId)

	pool.currentId += 1

	return clientId
}

func (pool *clientPool) disconnect(clientId int) {
	pool.wg.Done()
	delete(pool.active, clientId)

	log.Printf("Client %d disconnected", clientId)
}

func handler(conn net.Conn, storage core.Storage, clients *clientPool, clientId int, stopChan chan struct{}) {
	defer func() {
		conn.Close()
		clients.disconnect(clientId)
	}()

	conn.SetReadDeadline(time.Now().Add(1e9))

	var (
		buf = make([]byte, 1024)
		reader = bufio.NewReader(conn)
		writer = bufio.NewWriter(conn)
	)

	conn.Write(welcome)

	writer.Write(welcome)
	writer.Flush()

	for {
		select {
		case <- stopChan:
			return
		default:
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
	checkpointScheduler := storage.RunCheckpointing()
	statsScheduler, metric := core.RunStats(storage)

	defer func() {
		listener.Close()

		checkpointScheduler.Stop()
		statsScheduler.Stop()

		metric.ShowPlot()
	}()

	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, os.Interrupt, os.Kill, syscall.SIGTERM)

	stopHandlerChan := make(chan struct{})

	clients := clientPool{
		wg: sync.WaitGroup{},
		active: make(map[int]bool),
	}

	for {
		select {
		case <-quitChan:
			log.Println("Shutting down...")
			if len(clients.active) > 0 {
				close(stopHandlerChan)
				clients.wg.Wait()
			}
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

			clientId := clients.connect()
			go handler(conn, storage, &clients, clientId, stopHandlerChan)
		}
	}
}

func main() {
	log.SetFlags(log.Lmicroseconds)
	SocketServer()
}

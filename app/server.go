package main

import (
	"flag"
	"fmt"
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
		log.Printf("Client %d disconnected", clientId)
		clientPool.Done()
		conn.Close()
	}()

	buf := make([]byte, 1024)

	conn.Write(welcome)

	for {
		select {
		case <- stopChan:
			return
		default:
			conn.SetReadDeadline(time.Now().Add(1e9))

			n, err := conn.Read(buf)
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
			reply := core.ProcessQuery(query, storage)

			conn.Write(reply)
		}
	}
}

func SocketServer(hostname string, port *string) {
	var (
		quitChan = make(chan os.Signal, 1)
		stopHandlerChan = make(chan struct{})
		clientPool = sync.WaitGroup{}
	)

	storage := core.InitStorage()
	checkpointer := core.RunCheckpointing(storage)

	addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%s", hostname, *port))
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Fatalf("Socket listen port %s failed, %s", *port, err)
	}

	log.Printf("Begin listen to port: %s\n", *port)

	signal.Notify(quitChan, os.Interrupt, os.Kill, syscall.SIGTERM)

	for {
		select {
		case <-quitChan:
			log.Println("Shutting down...")

			//wait for the next scheduled checkpoint and then stop accepting queries
			checkpointer.Quit <- true
			<- checkpointer.Quit
			checkpointer.Scheduler.Stop()

			close(stopHandlerChan)
			clientPool.Wait()

			listener.Close()
			storage.Stop()

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

	port := flag.String("p", "8080", "port")
	flag.Parse()

	hostname, _ := os.Hostname()

	// dirty hack
	if hostname == "diufmac48.unifr.ch" {
		hostname = "localhost"
	}

	SocketServer(hostname, port)
}

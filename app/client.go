package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
)

func SocketClient(host *string, port *string) {
	addr := net.JoinHostPort(*host, *port)
	conn, err := net.Dial("tcp", addr)

	defer conn.Close()

	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Connected to port %s\n\n", *port)

	buff := make([]byte, 1024)
	n, _ := conn.Read(buff)
	fmt.Printf("%s\n", buff[:n])

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		conn.Write([]byte(scanner.Text()))

		buff := make([]byte, 1024)
		n, _ := conn.Read(buff)
		fmt.Printf("%s\n", buff[:n])
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error: %s", err)
	}
}

func main() {
	host := flag.String("host", "localhost", "host")
	port := flag.String("port", "8080", "port")

	flag.Parse()

	SocketClient(host, port)
}
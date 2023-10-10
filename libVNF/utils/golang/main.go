package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	dh "main/dataHandler"
)

var concurrentMap = sync.Map{} // Create a concurrent map to manage connections
var socketDir = "/home/kailian/libVNF/vnf/tmp/%d.sock"
var socketFileNames = []int{1, 2, 3, 4}
var sockets = make([]net.Listener, len(socketFileNames))

type RetPacket struct {
	length  int32
	context string
}

func (r *RetPacket) toByte() []byte {
	if r.length == 0 && r.context != "" {
		r.length = int32(len(r.context))
	}
	t := append([]byte(string(r.length)), r.context...)
	return t
}

func ReadAll(conn net.Conn) (string, error) {
	reader := bufio.NewReader(conn)
	var result string

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return result, err
		}

		result += line

		// Modify or add your own termination condition here
		if line == "END\n" {
			break
		}
	}

	return result, nil
}

func handleConnection(i int, conn net.Conn, cm *sync.Map) {
	defer conn.Close()

	// Read All would return EOF as error. But streams ends with EOF.
	// Read the first int
	var intData int
	var lenBuf = []byte{0, 0, 0, 0}
	// Decode as little endian.
	for {
		// err := binary.Read(conn, binary.LittleEndian, &intData)
		if _, err := conn.Read(lenBuf); err != nil {
			panic(err)
		}
		intData = int(binary.LittleEndian.Uint32(lenBuf))
		if intData != 0 {
			break
		}
	}

	// Read the string of specified length
	sb := make([]byte, int(intData))
	// conn.SetReadDeadline(time.Now().Add(1 * time.Microsecond))
	num, err := conn.Read(sb)
	if num != intData {
		fmt.Println("Frame corrupted.")
	} else {
		fmt.Printf("read size of %d\n", num)
	}
	if err != nil {
		panic(err)
	}

	ds := dh.DSPacketHandler{}
	ds.Data = sb
	ds.Len = intData

	var socketId int
	var cmd string
	var table string
	var key int

	ds.ExtractItem(&socketId)
	ds.ExtractItem(&cmd)
	ds.ExtractItem(&table)
	ds.ExtractItem(&key)

	// Disposal logic.
	var ret interface{}
	var ok bool
	if cmd == "get" {
		if ret, ok = (*cm).Load(table); !ok {
			panic("handle connection fetch.")
		}
	} else if cmd == "set" {
		// Not clear yet.
		(*cm).Store(table, key)
	} else {
		panic("Unknown instruct.")
	}

	// Re construct sending ds.
	ds = dh.DSPacketHandler{}

	// Deconstruct r.
	if r, o := ret.(string); !o {
		panic("handler type assertioa.")
	} else {
		// Make use of r and return.
		ds.Data = make([]byte, 100)
		ds.AppendItem(0)        // Reserve space for length.
		ds.AppendItem(socketId) // Reserve space for length.
		ds.AppendItem(key)      // Key.
		ds.AppendItem(r)        // Buffer
		ds.PrependLength()
	}

	conn.Write(ds.Data)

	// Placeholder: Simulating some processing and updating the map
	// key := "example_key"
	// value := "example_value"
	// (*cm).Store(key, value)

	// Placeholder: Simulating some response
	// response := "Request processed successfully"
	// conn.Write([]byte(response))
}

func initDataStore() {
	concurrentMap.Store("min_server", "127.0.0.1:6061")
}

func main() {

	initDataStore()

	// Remove the existing socket file (if any)

	for _, i := range socketFileNames {
		go func(i int) {
			socketPath := fmt.Sprintf(socketDir, i)
			// Listen for incoming connections
			var err error
			os.Remove(socketPath)
			sockets[i-1], err = net.Listen("unix", socketPath)
			if err != nil {
				fmt.Println("Error listening:", err)
				return
			}
			defer sockets[i-1].Close()
			fmt.Println("UDS Server listening on", socketPath)
			// Handle OS signals to gracefully shutdown the server

			sigChannel := make(chan os.Signal, 1)
			signal.Notify(sigChannel, os.Interrupt, syscall.SIGTERM)
			go func() {
				<-sigChannel
				sockets[i-1].Close()
				os.Remove(socketPath)
				fmt.Println("Server shut down.")
				os.Exit(0)
			}()
			for {
				conn, err := sockets[i-1].Accept()
				if err != nil {
					fmt.Println("Error accepting connection:", err)
					os.Exit(-1)
				}

				// Launch a new goroutine to handle the connection
				go handleConnection(i, conn, &concurrentMap)
			}
		}(i)
	}
	select {}
}

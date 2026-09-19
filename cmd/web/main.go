// Command web serves the Arachne web dashboard and results on localhost.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
)

func main() {
	portFlag := flag.String("port", "8080", "Port to serve dashboard on")
	flag.Parse()

	port := *portFlag
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	// Serve static files from the repository root (index.html, results.json)
	fs := http.FileServer(http.Dir("."))
	http.Handle("/", fs)

	fmt.Println("🕷️  Arachne Dashboard Server")
	fmt.Printf("🌐 Running at: http://localhost%s\n", port)
	fmt.Println("Press Ctrl+C to stop the server")

	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

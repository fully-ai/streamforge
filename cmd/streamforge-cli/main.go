package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/fully-ai/streamforge/internal/broker"
	"github.com/fully-ai/streamforge/pkg/client"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: streamforge-cli <create-topic|produce|consume> [args...]")
		os.Exit(1)
	}
	addr := os.Getenv("STREAMFORGE_WIRE")
	if addr == "" {
		addr = "127.0.0.1:9092"
	}
	c, err := client.Dial(addr, os.Getenv("STREAMFORGE_AUTH_TOKEN"), 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer c.Close()

	switch os.Args[1] {
	case "create-topic":
		if len(os.Args) < 3 {
			os.Exit(1)
		}
		parts := 3
		if len(os.Args) > 3 {
			parts, _ = strconv.Atoi(os.Args[3])
		}
		if err := c.CreateTopic(broker.TopicConfig{Name: os.Args[2], Partitions: parts}); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Topic created:", os.Args[2])
	case "produce":
		if len(os.Args) < 4 {
			os.Exit(1)
		}
		res, err := c.Produce(os.Args[2], []byte(os.Args[3]), []byte(os.Args[len(os.Args)-1]))
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Produced: %+v\n", res)
	case "consume":
		if len(os.Args) < 3 {
			os.Exit(1)
		}
		part := 0
		if len(os.Args) > 3 {
			part, _ = strconv.Atoi(os.Args[3])
		}
		recs, err := c.Fetch(os.Args[2], part, 0, 100)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		for _, r := range recs {
			fmt.Printf("offset=%d key=%s val=%s\n", r.Offset, string(r.Key), string(r.Value))
		}
	}
}

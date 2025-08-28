// reader.go
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"orderbook/orderbook" // protoc로 생성한 패키지
)

type OrderBook struct {
	Bids map[float64]float64 // 가격(key)과 수량(value)
	Asks map[float64]float64
}

func main() {
	symbol := "ETHUSDT"
	targetTime := time.Date(2025, 8, 26, 15, 13, 6, 0, time.UTC).UnixMilli()

	dateStr := time.UnixMilli(targetTime).UTC().Format("2006-01-02")
	fileName := fmt.Sprintf("data/%s/%s_%s.bin", strings.ToLower(symbol), strings.ToLower(symbol), dateStr)

	log.Printf("Attempting to find order book for %s at %d from file %s", symbol, targetTime, fileName)

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Failed to open file %s: %v", fileName, err)
	}
	defer file.Close()

	var closestSnapshot *orderbook.Snapshot

	for {
		snapshot, err := readNextSnapshot(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading snapshot, skipping: %v", err)
			continue
		}

		if snapshot.EventTime > targetTime {
			break
		}

		closestSnapshot = snapshot
	}

	if closestSnapshot == nil {
		log.Fatal("No snapshot found before the target time. Try an earlier time or check if the file has data.")
	}

	log.Printf("Found closest snapshot with EventTime: %d (diff: %dms)", closestSnapshot.EventTime, targetTime-closestSnapshot.EventTime)

	book := &OrderBook{
		Bids: make(map[float64]float64),
		Asks: make(map[float64]float64),
	}
	for _, l := range closestSnapshot.Bids {
		book.Bids[l.Price] = l.Quantity
	}
	for _, l := range closestSnapshot.Asks {
		book.Asks[l.Price] = l.Quantity
	}

	fmt.Printf("\n--- Order Book for %s at %s ---\n", symbol, time.UnixMilli(targetTime).UTC())
	printBook(book, 20)
}

func readNextSnapshot(f *os.File) (*orderbook.Snapshot, error) {
	lenBuf := make([]byte, 4)
	_, err := io.ReadFull(f, lenBuf)
	if err != nil {
		return nil, err
	}

	msgLen := binary.LittleEndian.Uint32(lenBuf)
	msgBuf := make([]byte, msgLen)
	_, err = io.ReadFull(f, msgBuf)
	if err != nil {
		return nil, err
	}

	var snapshot orderbook.Snapshot
	if err := proto.Unmarshal(msgBuf, &snapshot); err != nil {
		return nil, err
	}

	return &snapshot, nil
}

func printBook(book *OrderBook, depth int) {
	askPrices := make([]float64, 0, len(book.Asks))
	for p := range book.Asks {
		askPrices = append(askPrices, p)
	}
	sort.Float64s(askPrices)

	fmt.Println("------------- Asks -------------")
	fmt.Println("Price\t\tQuantity")
	// 가장 낮은 가격부터 출력 (오름차순)
	for i := 0; i < depth && i < len(askPrices); i++ {
		p := askPrices[i]
		fmt.Printf("%.4f\t%.4f\n", p, book.Asks[p])
	}

	bidPrices := make([]float64, 0, len(book.Bids))
	for p := range book.Bids {
		bidPrices = append(bidPrices, p)
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(bidPrices)))

	fmt.Println("------------- Bids -------------")
	fmt.Println("Price\t\tQuantity")
	// 가장 높은 가격부터 출력 (내림차순)
	for i := 0; i < depth && i < len(bidPrices); i++ {
		p := bidPrices[i]
		fmt.Printf("%.4f\t%.4f\n", p, book.Bids[p])
	}
}

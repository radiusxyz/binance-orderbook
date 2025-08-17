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

// 메모리에서 오더북을 관리하기 위한 구조체
type OrderBook struct {
	Bids map[float64]float64 // 가격(key)과 수량(value)
	Asks map[float64]float64
}

func main() {
	// --- 설정 ---
	symbol := "ETHUSDT"
	// 복원하고 싶은 목표 시간 (UTC)
	targetTime := time.Date(2025, 8, 17, 5, 13, 6, 0, time.UTC).UnixMilli()
	// --- 설정 끝 ---

	// 소문자 및 data/ 폴더 경로 적용
	dateStr := time.UnixMilli(targetTime).UTC().Format("2006-01-02")
	fileName := fmt.Sprintf("data/%s/%s_%s.bin", strings.ToLower(symbol), strings.ToLower(symbol), dateStr)

	log.Printf("Attempting to find order book for %s at %d from file %s", symbol, targetTime, fileName)

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Failed to open file %s: %v", fileName, err)
	}
	defer file.Close()

	var closestSnapshot *orderbook.Snapshot

	// 파일을 한 번만 스캔하여 목표 시간 직전의 마지막 스냅샷을 찾습니다.
	for {
		snapshot, err := readNextSnapshot(file)
		fmt.Printf("time: %d\n", snapshot.EventTime)
		if err == io.EOF {
			break // 파일 끝에 도달하면 종료
		}
		if err != nil {
			log.Printf("Error reading snapshot, skipping: %v", err)
			continue
		}

		// 스냅샷 시간이 목표 시간을 넘어서면, 바로 직전에 찾은 스냅샷이 정답이므로 루프를 중단합니다.
		if snapshot.EventTime > targetTime {
			break
		}

		// 현재 스냅샷이 유효한 후보이므로 저장해 둡니다.
		closestSnapshot = snapshot
	}

	if closestSnapshot == nil {
		log.Fatal("No snapshot found before the target time. Try an earlier time or check if the file has data.")
	}

	log.Printf("Found closest snapshot with EventTime: %d (diff: %dms)", closestSnapshot.EventTime, targetTime-closestSnapshot.EventTime)

	// 찾은 스냅샷을 OrderBook 구조체로 변환
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
	printBook(book, 20) // 상위 20개 호가 전체를 출력
}

// 파일에서 다음 length-prefixed Snapshot 메시지를 읽는 헬퍼 함수
func readNextSnapshot(f *os.File) (*orderbook.Snapshot, error) {
	lenBuf := make([]byte, 4)
	_, err := io.ReadFull(f, lenBuf)
	if err != nil {
		return nil, err // io.EOF 포함
	}

	msgLen := binary.LittleEndian.Uint32(lenBuf)
	msgBuf := make([]byte, msgLen)
	_, err = io.ReadFull(f, msgBuf)
	if err != nil {
		return nil, err
	}

	// 이제 Event가 아닌 Snapshot 메시지를 직접 파싱
	var snapshot orderbook.Snapshot
	if err := proto.Unmarshal(msgBuf, &snapshot); err != nil {
		return nil, err
	}

	return &snapshot, nil
}

// 오더북을 정렬하여 출력하는 함수
func printBook(book *OrderBook, depth int) {
	askPrices := make([]float64, 0, len(book.Asks))
	for p := range book.Asks {
		askPrices = append(askPrices, p)
	}
	sort.Float64s(askPrices) // 오름차순 정렬

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
	sort.Sort(sort.Reverse(sort.Float64Slice(bidPrices))) // 내림차순 정렬

	fmt.Println("------------- Bids -------------")
	fmt.Println("Price\t\tQuantity")
	// 가장 높은 가격부터 출력 (내림차순)
	for i := 0; i < depth && i < len(bidPrices); i++ {
		p := bidPrices[i]
		fmt.Printf("%.4f\t%.4f\n", p, book.Bids[p])
	}
}

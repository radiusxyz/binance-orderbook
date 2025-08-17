package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
	"orderbook/orderbook"
)

const (
	websocketURL = "wss://stream.binance.com:9443/stream?streams="
	streamSuffix = "@depth20@100ms" // 상위 20개, 100ms 주기 스냅샷 스트림
)

var symbols = []string{"ethusdt", "ethusdc", "ethbtc"}

// --- 구조체 정의 ---
type CombinedStreamEvent struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// Partial Depth Stream 응답 구조체 (스냅샷)
type SnapshotEvent struct {
	LastUpdateID int64       `json:"lastUpdateId"`
	Bids         [][2]string `json:"bids"`
	Asks         [][2]string `json:"asks"`
}

type FileManager struct {
	mu           sync.Mutex
	fileWriters  map[string]*bufio.Writer
	openFiles    map[string]*os.File
	currentDates map[string]string
}

// (FileManager 및 헬퍼 함수들은 이전과 거의 동일)
func NewFileManager() *FileManager {
	return &FileManager{
		fileWriters:  make(map[string]*bufio.Writer),
		openFiles:    make(map[string]*os.File),
		currentDates: make(map[string]string),
	}
}

func (fm *FileManager) getWriter(symbol string) (*bufio.Writer, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	const dataDir = "data"
	utcDate := time.Now().UTC().Format("2006-01-02")
	symbolLower := strings.ToLower(symbol)
	if fm.currentDates[symbolLower] != utcDate {
		if file, ok := fm.openFiles[symbolLower]; ok {
			fm.fileWriters[symbolLower].Flush()
			file.Close()
		}
		fullDirPath := fmt.Sprintf("%s/%s", dataDir, symbolLower)
		if err := os.MkdirAll(fullDirPath, os.ModePerm); err != nil {
			return nil, err
		}
		fileName := fmt.Sprintf("%s/%s_%s.bin", fullDirPath, symbolLower, utcDate)
		file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		fm.openFiles[symbolLower] = file
		fm.fileWriters[symbolLower] = bufio.NewWriter(file)
		fm.currentDates[symbolLower] = utcDate
		log.Printf("Opened new data file for %s: %s", symbolLower, fileName)
	}
	return fm.fileWriters[symbolLower], nil
}

func (fm *FileManager) writeSnapshot(symbol string, snapshot *orderbook.Snapshot) {
	writer, err := fm.getWriter(symbol)
	if err != nil {
		log.Printf("Error getting writer for %s: %v", symbol, err)
		return
	}
	bytes, err := proto.Marshal(snapshot)
	if err != nil {
		log.Printf("Error marshalling proto: %v", err)
		return
	}
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(bytes)))
	fm.mu.Lock()
	defer fm.mu.Unlock()
	writer.Write(lenBuf)
	writer.Write(bytes)
	writer.Flush()
}

func main() {
	fmt.Printf("%d\n", time.Now().UTC().UnixMilli())
	fm := NewFileManager()
	// 자동 재연결을 위한 무한 루프
	for {
		runCollector(fm)
		log.Printf("Disconnected. Reconnecting in 5 seconds...")
		time.Sleep(5 * time.Second)
	}
}

func runCollector(fm *FileManager) {
	var streamNames []string
	for _, s := range symbols {
		streamNames = append(streamNames, s+streamSuffix)
	}
	fullURL := websocketURL + strings.Join(streamNames, "/")

	conn, _, err := websocket.DefaultDialer.Dial(fullURL, nil)
	if err != nil {
		log.Printf("WebSocket dial error: %v", err)
		return
	}
	defer conn.Close()

	conn.SetPingHandler(func(appData string) error {
		log.Println("Received Ping, sending Pong.")
		return conn.WriteMessage(websocket.PongMessage, []byte(appData))
	})

	log.Printf("Connected to combined stream: %s", fullURL)

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("WebSocket read error: %v", err)
			return
		}
		var streamEvent CombinedStreamEvent
		if err := json.Unmarshal(message, &streamEvent); err != nil {
			log.Println("Combined stream unmarshal error:", err)
			continue
		}

		var snapshot SnapshotEvent
		if err := json.Unmarshal(streamEvent.Data, &snapshot); err != nil {
			log.Println("Snapshot data from stream unmarshal error:", err)
			continue
		}

		symbolFromStream := strings.Split(streamEvent.Stream, "@")[0]

		//fmt.Printf("sym(%s) %d\n", symbolFromStream, time.Now().UTC().UnixMilli())
		// 받은 스냅샷을 Protobuf 메시지로 변환
		pbSnapshot := &orderbook.Snapshot{
			EventTime:    time.Now().UTC().UnixMilli(), // 스트림에 타임스탬프가 없으므로 수신 시간 사용
			LastUpdateId: snapshot.LastUpdateID,
			Bids:         parseLevels(snapshot.Bids),
			Asks:         parseLevels(snapshot.Asks),
		}

		fm.writeSnapshot(symbolFromStream, pbSnapshot)
	}
}

func parseLevels(levels [][2]string) []*orderbook.Level {
	pbLevels := make([]*orderbook.Level, len(levels))
	for i, l := range levels {
		price, _ := strconv.ParseFloat(l[0], 64)
		qty, _ := strconv.ParseFloat(l[1], 64)
		pbLevels[i] = &orderbook.Level{Price: price, Quantity: qty}
	}
	return pbLevels
}

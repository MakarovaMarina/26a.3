package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const bufferSize = 5
const flushInterval = 2 * time.Second

type RingBuffer struct {
	buffer    []int
	size      int
	writePos  int
	readPos   int
	mu        sync.Mutex
	flushChan chan []int
	ticker    *time.Ticker
}

func NewRingBuffer(size int, interval time.Duration) *RingBuffer {
	rb := &RingBuffer{
		buffer:    make([]int, size),
		size:      size,
		flushChan: make(chan []int),
		ticker:    time.NewTicker(interval),
	}
	go rb.periodicFlush()
	return rb
}

func (rb *RingBuffer) Add(value int) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.buffer[rb.writePos] = value
	rb.writePos = (rb.writePos + 1) % rb.size
	if rb.writePos == rb.readPos {
		rb.readPos = (rb.readPos + 1) % rb.size
	}
}

func (rb *RingBuffer) Flush() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.writePos == rb.readPos {
		return
	}

	var data []int
	if rb.writePos > rb.readPos {
		data = append(data, rb.buffer[rb.readPos:rb.writePos]...)
	} else {
		data = append(data, rb.buffer[rb.readPos:]...)
		data = append(data, rb.buffer[:rb.writePos]...)
	}
	rb.readPos = rb.writePos
	rb.flushChan <- data
}

func (rb *RingBuffer) periodicFlush() {
	for range rb.ticker.C {
		rb.Flush()
	}
}

func (rb *RingBuffer) Stop() {
	rb.ticker.Stop()
	close(rb.flushChan)
}

func (rb *RingBuffer) FlushChan() <-chan []int {
	return rb.flushChan
}

func dataSource(out chan<- int) {
	scanner := bufio.NewScanner(os.Stdin)
	log.Println("Введите целые числа. Для выхода введите 'exit':")
	for scanner.Scan() {
		input := scanner.Text()
		if input == "exit" {
			close(out)
			return
		}
		if num, err := strconv.Atoi(input); err == nil {
			out <- num
		} else {
			log.Println("Некорректное значение. Введите целое число.")
		}
	}
}

func filterNegativeNumbers(in <-chan int, out chan<- int) {
	for num := range in {
		if num >= 0 {
			out <- num
		}
	}
	close(out)
}

func filterNonMultiplesOfThree(in <-chan int, out chan<- int) {
	for num := range in {
		if num != 0 && num%3 == 0 {
			out <- num
		}
	}
	close(out)
}

func bufferStage(in <-chan int, rb *RingBuffer) {
	for num := range in {
		rb.Add(num)
	}
	rb.Stop()
}

func dataConsumer(in <-chan []int) {
	for batch := range in {
		for _, data := range batch {
			log.Printf("Получены данные: %d\n", data)
		}
	}
}

func main() {
	sourceChan := make(chan int)
	filteredNegativeChan := make(chan int)
	filteredMultiplesOfThreeChan := make(chan int)

	ringBuffer := NewRingBuffer(bufferSize, flushInterval)

	go dataSource(sourceChan)
	go filterNegativeNumbers(sourceChan, filteredNegativeChan)
	go filterNonMultiplesOfThree(filteredNegativeChan, filteredMultiplesOfThreeChan)
	go bufferStage(filteredMultiplesOfThreeChan, ringBuffer)

	dataConsumer(ringBuffer.FlushChan())
}

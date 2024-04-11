package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/FadyGamilH/goq/client"
)

const (
	// maxGeneratedNums = 10000000
	// maxBatchSize     = models.MB
	maxGeneratedNums = 200
	maxBatchSize     = 10
)

func main() {
	log.Println("===============================================================================================================")
	log.Println("This pkg tests the client pkg implemented for [GoQ] and ensure that we can publish and consume events correctly")
	log.Println("===============================================================================================================")

	goq := client.NewGoq([]string{"localhost"})

	if sum, err := testProduce(goq); err != nil {
		log.Fatalf(err.Error())
	} else {
		log.Println("sum of produced numbers is : ", sum)
	}

	sum, err := testConsume(goq)
	if err != nil {
		log.Fatalf(err.Error())
	} else {
		log.Println("sum of consumed numbers is : ", sum)
	}
}

func testProduce(q *client.GoQ) (int64, error) {
	b := &bytes.Buffer{}
	sum := int64(0)
	for i := 0; i <= maxGeneratedNums; i++ {
		sum += int64(i)
		fmt.Fprintf(b, "%d\n", i)
		// produce batch of data once we hit the maxBtachSize
		if b.Len() >= maxBatchSize {
			if err := q.Produce(b.Bytes()[0:maxBatchSize]); err != nil {
				return 0, errors.New("ERROR_PRODUCING_DATA." + err.Error())
			}
			b.Reset()
		}
	}
	// we still have data in the buffer but doesn't exceeds or equales the batchSize so these data aren't produced yet, so we need to produce them
	if b.Len() != 0 {
		if err := q.Produce(b.Bytes()); err != nil {
			return 0, errors.New("ERROR_PRODUCING_DATA." + err.Error())
		}
	}
	return sum, nil
}

func testConsume(q *client.GoQ) (int64, error) {
	b := make([]byte, maxBatchSize)
	sum := int64(0)
	for {
		res, err := q.Consume(b)
		if err != nil {
			if err == io.EOF {
				return sum, nil
			}
			return 0, errors.New("ERROR_CONSUME_DATA." + err.Error())
		}
		batchInStrings := strings.Split(string(res), "\n")
		log.Println(batchInStrings)
		for _, val := range batchInStrings {
			// notice that the last item produced is produced like the following (val\n) so we have "" at the end we need to avoid converting to int64
			if val == "" {
				continue
			}
			intVal, err := strconv.Atoi(val)
			if err != nil {
				return 0, err
			}
			sum += int64(intVal)
		}
	}
}

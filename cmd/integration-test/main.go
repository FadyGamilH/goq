package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/FadyGamilH/goq/client"
	"github.com/FadyGamilH/goq/models"
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
	for i := 0; i <= models.MaxGeneratedNums; i++ {
		sum += int64(i)
		fmt.Fprintf(b, "%d\n", i)
		// produce batch of data once we hit the maxBtachSize
		if b.Len() >= models.MaxBatchSize {
			if err := q.Produce(b.Bytes()); err != nil {
				return 0, err
			}
			b.Reset()
		}
	}
	// we still have data in the buffer but doesn't exceeds or equales the batchSize so these data aren't produced yet, so we need to produce them
	if b.Len() != 0 {
		if err := q.Produce(b.Bytes()); err != nil {
			return 0, err
		}
	}
	return sum, nil
}

func testConsume(q *client.GoQ) (int64, error) {
	b := make([]byte, models.MaxBatchSize)
	sum := int64(0)
	for {
		res, err := q.Consume(b)
		if err == io.EOF {
			return sum, nil
		} else if err != nil {
			return 0, err
		}
		batchInStrings := strings.Split(string(res), "\n")
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
		b = make([]byte, models.MaxBatchSize)
	}
}

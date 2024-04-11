package client

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/FadyGamilH/goq/models"
)

// This is the client program which can be connected to a set of GoQ servers (distributed)
type GoQ struct {
	Addrs             []string
	Data              bytes.Buffer
	DataFromPrevBatch bytes.Buffer
}

func NewGoq(servers_addrs []string) *GoQ {
	return &GoQ{
		Addrs: servers_addrs,
	}
}

func (q *GoQ) Produce(msg []byte) error {
	_, err := q.Data.Write(msg)
	if err != nil {
		return err
	}
	// log.Printf("current buffer length is : {%+v} and current buffer content is : {%s}\n", q.data.Len(), q.data.String())
	return nil
}

// Consume is used to read data from GoQ
// receive a buffer which is nil if you need to read data into the default size buffer (64 KB) or you can pass the buffer with the custom size you need to read the data into it
// returns []bytes and error
func (q *GoQ) Consume(buffer []byte) ([]byte, error) {
	if buffer == nil {
		buffer = make([]byte, models.DefaultBufferSize)
	}
	offsetOfLastByteIntoBuffer := 0
	// check if there is any data couldn't be consumed from the prev batch
	if q.DataFromPrevBatch.Len() > 0 {
		// check if the data from the prev batch is bigger than the buffer that will be used to read the data on, so we will return an error
		if q.DataFromPrevBatch.Len() > len(buffer) {
			return nil, errors.New(models.ErrorBufferSmallerThanData)
		}
		// read the data from prev batch into the buffer and handle error
		numOfReadBytes, err := q.DataFromPrevBatch.Read(buffer)
		if err != nil {
			return nil, fmt.Errorf("{%s} : %v", models.ErrorReadingDataFromBuffer, err)
		}
		offsetOfLastByteIntoBuffer += numOfReadBytes
		// reset the DataFromPrevBatch buffer
		q.DataFromPrevBatch.Reset()
	}
	// then read the new consumed batch into the buffer but from the offsetOfLastByteIntoBuffer to the end of the buffer
	_, err := q.Data.Read(buffer[offsetOfLastByteIntoBuffer:])
	if err != nil {
		return nil, fmt.Errorf("{%s} : %v", models.ErrorReadingDataFromBuffer, err)
	}
	dataOfCurrBatch, dataForNextBatch, err := ConsumeMaxBatchSizeFromBuffer(buffer)
	if err != nil {
		return nil, err
	}
	q.DataFromPrevBatch.Reset()
	q.DataFromPrevBatch.Write(dataForNextBatch)
	return dataOfCurrBatch, nil
}

func ConsumeMaxBatchSizeFromBuffer(buffer []byte) (dataOfCurrBatch, dataForNextBatch []byte, err error) {

}

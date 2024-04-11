package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"

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
	return nil
}

// Consume is used to read data from GoQ
// receive a buffer which is nil if you need to read data into the default size buffer (64 KB) or you can pass the buffer with the custom size you need to read the data into it
// returns []bytes and error
func (q *GoQ) Consume(buffer []byte) ([]byte, error) {
	if buffer == nil {
		buffer = make([]byte, models.MaxBatchSize)
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
	numOfReadBytes, err := q.Data.Read(buffer[offsetOfLastByteIntoBuffer:])
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("{%s} : %v", models.ErrorReadingDataFromBuffer, err)
	}
	if numOfReadBytes == 0 && err == io.EOF {
		return nil, io.EOF
	}
	// the only reason i slice the data from 0 to the numOfReadBytes + offsetOfLastByteIntoBuffer is because i don't need to depend on the user-code which will use this client pkg to reset the buffer before sending it to me in the next iteration, so i always specify the indecis of the current batch data so thats how we can ensure to not use any zeros or data from prev iteration while we process the current batch data
	dataOfCurrBatch, dataForNextBatch, err := ConsumeMaxBatchSizeFromBuffer(buffer[0 : numOfReadBytes+offsetOfLastByteIntoBuffer])
	if err != nil {
		return nil, err
	}
	q.DataFromPrevBatch.Reset()
	q.DataFromPrevBatch.Write(dataForNextBatch)
	return dataOfCurrBatch, nil
}

func ConsumeMaxBatchSizeFromBuffer(buffer []byte) (dataOfCurrBatch, dataForNextBatch []byte, err error) {
	if len(buffer) == 0 {
		return buffer, nil, nil
	}
	if buffer[len(buffer)-1] == '\n' {
		return buffer, nil, nil
	}
	lastSepartorIndex := bytes.LastIndexByte(buffer, '\n')
	if lastSepartorIndex == -1 {
		return nil, nil, errors.New(models.ErrorBufferSmallerThanData)
	}
	return buffer[0 : lastSepartorIndex+1], buffer[lastSepartorIndex+1:], nil
}

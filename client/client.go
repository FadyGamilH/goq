package client

import (
	"bytes"

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
	n, err := q.Data.Read(buffer)
	if err != nil {
		return nil, err
	}
	return buffer[0:n], nil
}

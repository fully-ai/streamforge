package client

import (
	"net"
	"time"

	"github.com/fully-ai/streamforge/internal/broker"
	"github.com/fully-ai/streamforge/internal/logstore"
	"github.com/fully-ai/streamforge/internal/protocol"
)

// Client connects to StreamForge's binary TCP broker socket.
type Client struct {
	conn      net.Conn
	authToken string
}

func Dial(addr, authToken string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, authToken: authToken}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) CreateTopic(cfg broker.TopicConfig) error {
	_, err := protocol.Call(c.conn, protocol.OpCreateTopic, protocol.WireEnvelope{
		AuthToken: c.authToken, TopicCfg: &cfg,
	})
	return err
}

func (c *Client) Produce(topic string, key, value []byte) ([]broker.ProduceResult, error) {
	rep, err := protocol.Call(c.conn, protocol.OpProduce, protocol.WireEnvelope{
		AuthToken: c.authToken,
		Topic:     topic,
		Messages:  []broker.ProduceMessage{{Key: key, Value: value}},
	})
	return rep.Produce, err
}

func (c *Client) Fetch(topic string, partition int, offset uint64, max int) ([]logstore.Record, error) {
	rep, err := protocol.Call(c.conn, protocol.OpFetch, protocol.WireEnvelope{
		AuthToken: c.authToken, Topic: topic, Partition: partition, Offset: offset, Max: max,
	})
	return rep.Records, err
}

func (c *Client) CommitOffset(groupID, topic string, partition int, offset uint64) error {
	_, err := protocol.Call(c.conn, protocol.OpCommitOffset, protocol.WireEnvelope{
		AuthToken: c.authToken, GroupID: groupID, Topic: topic, Partition: partition, Offset: offset,
	})
	return err
}

package protocol

import (
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"time"

	"github.com/fully-ai/streamforge/internal/broker"
	"github.com/fully-ai/streamforge/internal/logstore"
)

const (
	OpProduce      byte = 1
	OpFetch        byte = 2
	OpCommitOffset byte = 3
	OpMetadata     byte = 4
	OpCreateTopic  byte = 5
)

type WireEnvelope struct {
	AuthToken string                  `json:"auth_token,omitempty"`
	Topic     string                  `json:"topic,omitempty"`
	Partition int                     `json:"partition,omitempty"`
	Offset    uint64                  `json:"offset,omitempty"`
	Max       int                     `json:"max,omitempty"`
	GroupID   string                  `json:"group_id,omitempty"`
	Messages  []broker.ProduceMessage `json:"messages,omitempty"`
	TopicCfg  *broker.TopicConfig     `json:"topic_cfg,omitempty"`
}

type WireReply struct {
	OK      bool                   `json:"ok"`
	Error   string                 `json:"error,omitempty"`
	Produce []broker.ProduceResult `json:"produce,omitempty"`
	Records []logstore.Record      `json:"records,omitempty"`
	Topics  []broker.TopicInfo     `json:"topics,omitempty"`
}

// ServeWire starts StreamForge's length-prefixed binary TCP broker protocol server.
func ServeWire(ln net.Listener, b *broker.Broker, authToken string, stopCh <-chan struct{}) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-stopCh:
				return
			default:
				continue
			}
		}
		go handleWireConn(conn, b, authToken)
	}
}

func handleWireConn(conn net.Conn, b *broker.Broker, authToken string) {
	defer conn.Close()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		var hdr [5]byte
		if _, err := io.ReadFull(conn, hdr[:]); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(hdr[0:4])
		op := hdr[4]
		if length > 8*1024*1024 {
			return
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return
		}
		var env WireEnvelope
		if json.Unmarshal(buf, &env) != nil {
			_ = writeReply(conn, WireReply{OK: false, Error: "invalid wire JSON"})
			continue
		}
		if authToken != "" && subtle.ConstantTimeCompare([]byte(env.AuthToken), []byte(authToken)) != 1 {
			_ = writeReply(conn, WireReply{OK: false, Error: "unauthorized"})
			continue
		}

		var reply WireReply
		switch op {
		case OpCreateTopic:
			if env.TopicCfg == nil {
				reply = WireReply{Error: "missing topic_cfg"}
			} else if err := b.CreateTopic(*env.TopicCfg); err != nil {
				reply = WireReply{Error: err.Error()}
			} else {
				reply = WireReply{OK: true}
			}
		case OpProduce:
			res, err := b.Produce(env.Topic, env.Messages)
			if err != nil {
				reply = WireReply{Error: err.Error()}
			} else {
				reply = WireReply{OK: true, Produce: res}
			}
		case OpFetch:
			recs, err := b.Fetch(env.Topic, env.Partition, env.Offset, env.Max)
			if err != nil {
				reply = WireReply{Error: err.Error()}
			} else {
				reply = WireReply{OK: true, Records: recs}
			}
		case OpCommitOffset:
			if err := b.CommitOffset(env.GroupID, env.Topic, env.Partition, env.Offset); err != nil {
				reply = WireReply{Error: err.Error()}
			} else {
				reply = WireReply{OK: true}
			}
		case OpMetadata:
			reply = WireReply{OK: true, Topics: b.ListTopics()}
		default:
			reply = WireReply{Error: "unknown opcode"}
		}
		if writeReply(conn, reply) != nil {
			return
		}
	}
}

func writeReply(conn net.Conn, rep WireReply) error {
	raw, _ := json.Marshal(rep)
	var lbuf [4]byte
	binary.BigEndian.PutUint32(lbuf[:], uint32(len(raw)))
	if _, err := conn.Write(lbuf[:]); err != nil {
		return err
	}
	_, err := conn.Write(raw)
	return err
}

// Call sends one binary wire frame and decodes the response.
func Call(conn net.Conn, op byte, env WireEnvelope) (WireReply, error) {
	body, err := json.Marshal(env)
	if err != nil {
		return WireReply{}, err
	}
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(body)))
	hdr[4] = op
	if _, err := conn.Write(hdr[:]); err != nil {
		return WireReply{}, err
	}
	if _, err := conn.Write(body); err != nil {
		return WireReply{}, err
	}
	var lbuf [4]byte
	if _, err := io.ReadFull(conn, lbuf[:]); err != nil {
		return WireReply{}, err
	}
	rlen := binary.BigEndian.Uint32(lbuf[:])
	rbuf := make([]byte, rlen)
	if _, err := io.ReadFull(conn, rbuf); err != nil {
		return WireReply{}, err
	}
	var rep WireReply
	if err := json.Unmarshal(rbuf, &rep); err != nil {
		return WireReply{}, err
	}
	if !rep.OK {
		return rep, errors.New(rep.Error)
	}
	return rep, nil
}

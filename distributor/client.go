package distributor

import (
	"sync"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/distributor/adp"
	"golang.org/x/net/context"
)

const (
	clientTimeout          = 100 * time.Millisecond
	connectTimeout         = 2 * time.Second
	rebalanceClientTimeout = 5 * time.Second
	writeClientTimeout     = 2000 * time.Millisecond
)

// TODO(barakmich): Clean up errors

type distClient struct {
	dist *Distributor
	//TODO(barakmich): Better connection pooling
	openConns map[string]*adp.Conn
	mut       sync.Mutex
}

func newDistClient(d *Distributor) *distClient {

	client := &distClient{
		dist:      d,
		openConns: make(map[string]*adp.Conn),
	}
	d.srv.AddTimeoutCallback(client.onPeerTimeout)
	return client
}

func (d *distClient) onPeerTimeout(uuid string) {
	d.mut.Lock()
	defer d.mut.Unlock()
	conn, ok := d.openConns[uuid]
	if !ok {
		return
	}
	err := conn.Close()
	if err != nil {
		clog.Errorf("peer timeout err on close: %s", err)
	}
	delete(d.openConns, uuid)

}
func (d *distClient) getConn(uuid string) *adp.Conn {
	d.mut.Lock()
	defer d.mut.Unlock()
	if conn, ok := d.openConns[uuid]; ok {
		return conn
	}
	pm := d.dist.srv.GetPeerMap()
	pi := pm[uuid]
	if pi == nil {
		// We know this UUID exists, we don't have an address for it, let's refresh now.
		pm := d.dist.srv.UpdatePeerMap()
		pi = pm[uuid]
		if pi == nil {
			// Not much more we can try
			return nil
		}
	}
	if pi.TimedOut {
		return nil
	}
	d.mut.Unlock()
	conn, err := adp.Dial(pi.Address, connectTimeout, d.dist.BlockSize())
	d.mut.Lock()
	if err != nil {
		clog.Errorf("couldn't dial: %v", err)
		return nil
	}
	d.openConns[uuid] = conn
	return conn
}

func (d *distClient) Close() error {
	d.mut.Lock()
	defer d.mut.Unlock()
	for _, c := range d.openConns {
		err := c.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *distClient) GetBlock(ctx context.Context, uuid string, b agro.BlockRef) ([]byte, error) {
	conn := d.getConn(uuid)
	if conn == nil {
		return nil, agro.ErrNoPeer
	}
	data, err := conn.Block(ctx, b)
	if err != nil {
		clog.Debug(err)
		return nil, agro.ErrBlockUnavailable
	}
	return data, nil
}

func (d *distClient) PutBlock(ctx context.Context, uuid string, b agro.BlockRef, data []byte) error {
	conn := d.getConn(uuid)
	if conn == nil {
		return agro.ErrBlockUnavailable
	}
	newctx, cancel := context.WithTimeout(ctx, writeClientTimeout)
	defer cancel()
	err := conn.PutBlock(newctx, b, data)
	if err != nil {
		if err == context.DeadlineExceeded {
			return agro.ErrBlockUnavailable
		}
	}
	return err
}

func (d *distClient) Check(ctx context.Context, uuid string, blks []agro.BlockRef) ([]bool, error) {
	conn := d.getConn(uuid)
	if conn == nil {
		return nil, agro.ErrNoPeer
	}
	newctx, cancel := context.WithTimeout(ctx, rebalanceClientTimeout)
	defer cancel()
	resp, err := conn.RebalanceCheck(newctx, blks)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

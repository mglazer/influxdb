package meta

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/services/meta/internal"

	"github.com/gogo/protobuf/proto"
)

const errSleep = 10 * time.Millisecond

type Client struct {
	tls    bool
	logger *log.Logger

	mu          sync.RWMutex
	metaServers []string
	changed     chan struct{}
	closing     chan struct{}
	data        *Data

	executor *StatementExecutor
}

func NewClient(metaServers []string, tls bool) *Client {
	client := &Client{
		metaServers: metaServers,
		tls:         tls,
		logger:      log.New(os.Stderr, "[metaclient] ", log.LstdFlags),
	}
	client.executor = &StatementExecutor{Store: client}
	return client
}

func (c *Client) Open() error {
	c.changed = make(chan struct{})
	c.closing = make(chan struct{})
	c.data = c.retryUntilSnapshot()

	go c.pollForUpdates()

	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	close(c.closing)

	return nil
}

func (c *Client) ClusterID() (id uint64, err error) {
	return 0, nil
}

// Node returns a node by id.
func (c *Client) DataNode(id uint64) (*NodeInfo, error) {
	return nil, nil
}

func (c *Client) DataNodes() ([]NodeInfo, error) {
	return nil, nil
}

func (c *Client) DeleteDataNode(nodeID uint64) error {
	return nil
}

func (c *Client) MetaNodes() ([]NodeInfo, error) {
	return nil, nil
}

func (c *Client) Database(name string) (*DatabaseInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, d := range c.data.Databases {
		if d.Name == name {
			return &d, nil
		}
	}

	return nil, influxdb.ErrDatabaseNotFound(name)
}

func (c *Client) Databases() ([]DatabaseInfo, error) {
	return nil, nil
}

func (c *Client) CreateDatabase(name string) (*DatabaseInfo, error) {
	cmd := &internal.CreateDatabaseCommand{
		Name: proto.String(name),
	}

	ch := c.WaitForDataChanged()

	err := c.retryUntilExec(internal.Command_CreateDatabaseCommand, internal.E_CreateDatabaseCommand_Command, cmd)
	if err != nil {
		return nil, err
	}

	<-ch
	return c.Database(name)
}

func (c *Client) CreateDatabaseWithRetentionPolicy(name string, rpi *RetentionPolicyInfo) (*DatabaseInfo, error) {
	return nil, nil
}

func (c *Client) DropDatabase(name string) error {
	return nil
}

func (c *Client) CreateRetentionPolicy(database string, rpi *RetentionPolicyInfo) (*RetentionPolicyInfo, error) {
	return nil, nil
}

func (c *Client) CreateDatabaseIfNotExists(name string) (*DatabaseInfo, error) {
	return nil, nil
}

func (c *Client) CreateRetentionPolicyIfNotExists(database string, rpi *RetentionPolicyInfo) (*RetentionPolicyInfo, error) {
	return nil, nil
}

func (c *Client) DropRetentionPolicy(database, name string) error {
	return nil
}

func (c *Client) SetDefaultRetentionPolicy(database, name string) error {
	return nil
}

func (c *Client) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate) error {
	return nil
}

func (c *Client) IsLeader() bool {
	return false
}

func (c *Client) WaitForLeader(timeout time.Duration) error {
	return nil
}

func (c *Client) Users() (a []UserInfo, err error) {
	return nil, nil
}

func (c *Client) User(name string) (*UserInfo, error) {
	return nil, nil
}

func (c *Client) CreateUser(name, password string, admin bool) (*UserInfo, error) {
	return nil, nil
}

func (c *Client) UpdateUser(name, password string) error {
	return nil
}

func (c *Client) DropUser(name string) error {
	return nil
}

func (c *Client) SetPrivilege(username, database string, p influxql.Privilege) error {
	return nil
}

func (c *Client) SetAdminPrivilege(username string, admin bool) error {
	return nil
}

func (c *Client) UserPrivileges(username string) (map[string]influxql.Privilege, error) {
	return nil, nil
}

func (c *Client) UserPrivilege(username, database string) (*influxql.Privilege, error) {
	return nil, nil
}

func (c *Client) AdminUserExists() (bool, error) {
	return false, nil
}

func (c *Client) Authenticate(username, password string) (*UserInfo, error) {
	return nil, nil
}

func (c *Client) RetentionPolicy(database, name string) (rpi *RetentionPolicyInfo, err error) {
	return nil, nil
}

func (c *Client) VisitRetentionPolicies(f func(d DatabaseInfo, r RetentionPolicyInfo)) {

}

func (c *Client) UserCount() (int, error) {
	return 0, nil
}

func (c *Client) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []ShardGroupInfo, err error) {
	return nil, nil
}

func (c *Client) CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	return nil, nil
}

func (c *Client) DeleteShardGroup(database, policy string, id uint64) error {
	return nil
}

func (c *Client) PrecreateShardGroups(from, to time.Time) error {
	return nil
}

func (c *Client) ShardOwner(shardID uint64) (string, string, *ShardGroupInfo) {
	return "", "", nil
}

func (c *Client) CreateMetaNode(httpAddr, tcpAddr string) error {
	cmd := &internal.CreateMetaNodeCommand{
		HTTPAddr: proto.String(httpAddr),
		TCPAddr:  proto.String(tcpAddr),
	}

	return c.retryUntilExec(internal.Command_CreateMetaNodeCommand, internal.E_CreateMetaNodeCommand_Command, cmd)
}

func (c *Client) CreateContinuousQuery(database, name, query string) error {
	return nil
}

func (c *Client) DropContinuousQuery(database, name string) error {
	return nil
}

func (c *Client) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return nil
}

func (c *Client) DropSubscription(database, rp, name string) error {
	return nil
}

func (c *Client) ExecuteStatement(stmt influxql.Statement) *influxql.Result {
	return c.executor.ExecuteStatement(stmt)
}

// WaitForDataChanged will return a channel that will get closed when
// the metastore data has changed
func (c *Client) WaitForDataChanged() chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.changed
}

func (c *Client) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (c *Client) MetaServers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.metaServers
}

// retryUntilExec will attempt the command on each of the metaservers and return on the first success
func (c *Client) retryUntilExec(typ internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	var err error
	for _, s := range c.MetaServers() {
		err = c.exec(s, typ, desc, value)
		if err == nil {
			return nil
		}
	}
	return err
}

func (c *Client) exec(addr string, typ internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	// Create command.
	cmd := &internal.Command{Type: &typ}
	if err := proto.SetExtension(cmd, desc, value); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("://%s/execute", addr)
	if c.tls {
		url = "https" + url
	} else {
		url = "http" + url
	}

	resp, err := http.Post(url, "application/octet-stream", bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected result:\n\texp: %d\n\tgot: %d\n", http.StatusOK, resp.StatusCode)
	}

	return nil
}

func (c *Client) pollForUpdates() {
	for {
		data := c.retryUntilSnapshot()

		// update the data and notify of the change
		c.mu.Lock()
		idx := c.data.Index
		c.data = data
		if idx < data.Index {
			close(c.changed)
			c.changed = make(chan struct{})
		}
		c.mu.Unlock()
	}
}

func (c *Client) getSnapshot(server string, index uint64) (*Data, error) {
	url := fmt.Sprintf("://%s?index=%d", server, index)

	if c.tls {
		url = "https" + url
	} else {
		url = "http" + url
	}

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	data := &Data{}
	if err := data.UnmarshalBinary(b); err != nil {
		return nil, err
	}

	return data, nil
}

func (c *Client) retryUntilSnapshot() *Data {
	currentServer := 0
	for {
		// get the index to look from and the server to poll
		c.mu.RLock()
		idx := c.data.Index
		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		server := c.metaServers[currentServer]
		c.mu.RUnlock()

		data, err := c.getSnapshot(server, idx)

		if err == nil {
			return data
		}

		c.logger.Printf("failure getting snapshot from %s: %s", server, err.Error())

		currentServer += 1
		time.Sleep(errSleep)

		continue
	}
}

package cass

import (
	"errors"
	"fmt"
	"github.com/araddon/cass/cassandra"
	"github.com/pomack/thrift4go/lib/go/src/thrift"
	"log"
	"net"
	//"strconv"
	"strings"
	"sync"
	"time"
)

// The default logger is nil in Cassandra, and the default log level is
// 1= Error, to set them from within your app you set Logger, and LogLvel
//
//    func init()
//       cass.SetLogger(cass.DEBUG, log.New(os.Stdout, "", log.Ltime|log.Lshortfile))
//    }
//
const (
	FATAL = 0
	ERROR = 1
	WARN  = 2
	INFO  = 3
	DEBUG = 4
	None  = -1
)

var (
	Logger    *log.Logger
	logPrefix string = "CASS "
	LogColor         = map[int]string{FATAL: "\033[0m\033[37m",
		ERROR: "\033[0m\033[31m",
		WARN:  "\033[0m\033[33m",
		INFO:  "\033[0m\033[32m",
		DEBUG: "\033[0m\033[34m"}
	LogLevel int = ERROR
)

type CassandraError string

func (e CassandraError) Error() string { return "cass error " + string(e) }

func Debug(v ...interface{}) {
	DoLog(3, DEBUG, fmt.Sprint(v...), Logger)
}
func Debugf(format string, v ...interface{}) {
	DoLog(3, DEBUG, fmt.Sprintf(format, v...), Logger)
}
func Log(logLvl int, v ...interface{}) {
	if LogLevel >= logLvl {
		DoLog(3, logLvl, fmt.Sprint(v...), Logger)
	}
}
func doLog(msg string, logger *log.Logger) {
	if logger != nil {
		Logger.Output(3, msg)
	}
}
func DoLog(depth, logLvl int, msg string, lgr *log.Logger) {
	if lgr != nil {
		lgr.Output(depth, logPrefix+LogColor[logLvl]+msg+"\033[0m")
	}
}

func Logf(logLvl int, format string, v ...interface{}) {
	if LogLevel >= logLvl {
		DoLog(3, logLvl, fmt.Sprintf(format, v...), Logger)
	}
}
func SetLogger(logLevel int, logger *log.Logger) {
	Logger = logger
	LogLevel = logLevel
}

type CassandraConnection struct {
	Keyspace string
	Server   string
	Id       int
	pool     chan *CassandraConnection
	Client   *cassandra.CassandraClient

	// Sometimes when we open a cassandra connection, we can't set the keyspace because it
	// doesn't exist yet (the keyspace is set by each connection). When we execute a
	// successful "set keyspace" command, then this will be set to true.
	hasSetKeyspace bool
}

type KeyspaceConfig struct {
	servers     []string
	MaxPoolSize int // Per server
	// mu          sync.Mutex
	//ServerCount int
	//next int  // next server
	Keyspace string
	pool     chan *CassandraConnection
}

type KeyspaceConfigMap map[string]*KeyspaceConfig

var configMu sync.Mutex
var configMap = make(KeyspaceConfigMap)

func ConfigKeyspace(keyspace string, serverlist []string, poolsize int) *KeyspaceConfig {
	config := &KeyspaceConfig{servers: serverlist, Keyspace: keyspace, MaxPoolSize: poolsize}
	config.makePool()
	configMu.Lock()
	defer configMu.Unlock()
	configMap[keyspace] = config
	return config
}

// gives number of connectios available in pool
func (c *KeyspaceConfig) PoolSize() int {
	return len(c.pool)
}

// create connection pool, initialize connections 
func (c *KeyspaceConfig) makePool() {

	c.pool = make(chan *CassandraConnection, c.MaxPoolSize*len(c.servers))

	for i := 0; i < c.MaxPoolSize; i++ {
		for whichServer := 0; whichServer < len(c.servers); whichServer++ {
			// add empty values to the pool
			// c.pool <- &CassandraConnection{Keyspace: c.Keyspace, Id: i, Server: c.servers[whichServer]}
			addConnToPool(c.pool, c.Keyspace, i, c.servers[whichServer])
		}
	}
}

func addConnToPool(pool chan *CassandraConnection, keyspace string, id int, server string) {
	pool <- &CassandraConnection{Keyspace: keyspace, Id: id, Server: server}
}

func ConnPoolSize(keyspace string) int {
	configMu.Lock()
	defer configMu.Unlock()
	config, ok := configMap[keyspace]
	if ok {
		return len(config.pool)
	}
	return 0
}

// clean up and close connections
func CloseAll() {
	var conn *CassandraConnection

	configMu.Lock()
	defer configMu.Unlock()

	for _, c := range configMap {

		// TODO:  this is flawed, only closes non-checkedout connections?  
		//  Maybe close the channel?
		pool := c.pool
		if len(pool) > 0 {
			for i := 0; i < len(pool); i++ {
				conn = <-pool
				conn.Close()
			}
		}
	}
}

// main entry point for checking out a connection from a pre-defined (see #ConfigKeyspace)
// cassandra keyspace/server info
func GetCassConn(keyspace string) (conn *CassandraConnection, err error) {
	configMu.Lock()
	keyspaceConfig, ok := configMap[keyspace]
	if !ok {
		configMu.Unlock()
		return nil, errors.New("Must define keyspaces before you can get connection")
	}
	//keyspaceConfig.mu.Lock()
	//defer keyspaceConfig.mu.Unlock()
	configMu.Unlock()

	return getConnFromPool(keyspace, keyspaceConfig.pool)
}

func getConnFromPool(ks string, pool chan *CassandraConnection) (conn *CassandraConnection, err error) {

	conn = <-pool
	Logf(DEBUG, "in checkout, pulled off pool: remaining = %d, connid=%d Server=%s", len(pool), conn.Id, conn.Server)
	// BUG(ar):  an error occured on batch mutate <nil> <nil> <nil> Cannot read. Remote side has closed. Tried to read 4 bytes, but only got 0 bytes.
	if conn.Client == nil || conn.Client.Transport.IsOpen() == false {
		conn.pool = pool
		if err = conn.Open(ks); err != nil {
			// When a connection has an error, instead of returning it to the pool we'll add a
			// fresh connection to the pool that will hopefully be error-free
			conn.Close()
			addConnToPool(pool, ks, conn.Id, conn.Server)
			Log(ERROR, "Failed opening connection:", err.Error())
			return nil, err
		}

		Log(DEBUG, " in create conn, how is client? ", conn.Client, " is err? ", err)
	}

	// This should never happen, but to be safe we'll check and return an error
	if conn != nil && conn.Keyspace != ks {
		// When a connection has an error, instead of returning it to the pool we'll add a
		// fresh connection to the pool that will hopefully be error-free
		addConnToPool(pool, ks, conn.Id, conn.Server)
		conn.Close()
		return nil, fmt.Errorf("keyspace didn't match expected val %s: %s")
	}

	// The conn may never have run a "set keyspace" command, try again now if needed. If this
	// fails, we'll proceed anyway so operations like "create keyspace" can still be run.
	if err := conn.RequireKeyspaceSet(); err != nil {
		Log(WARN, "Keyspace couldn't be set when checking out connection")
	}

	return conn, nil
}

// opens a cassandra connection
func (conn *CassandraConnection) Open(keyspace string) error {

	Log(DEBUG, "creating new cassandra connection ", conn.pool)
	tcpConn, err := net.Dial("tcp", conn.Server)
	if err != nil {
		return err
	}

	ts, err := thrift.NewTSocketConn(tcpConn)
	if err != nil || ts == nil {
		return fmt.Errorf("Failed opening thrift socket: %s %s", err, ts)
	}

	if ts == nil {
		return errors.New("No TSocket connection?")
	}

	// the TSocket implements interface TTransport
	trans := thrift.NewTFramedTransport(ts)

	// A previous version of this code called trans.Open() at this point, but it's already open

	protocolfac := thrift.NewTBinaryProtocolFactoryDefault()

	conn.Client = cassandra.NewCassandraClientFactory(trans, protocolfac)

	Log(DEBUG, " in conn.Open, how is client? ", conn.Client)

	ire, t3e := conn.Client.SetCqlVersion("3.0.0")
	if ire != nil || t3e != nil {
		Log(ERROR, ire, t3e)
		return fmt.Errorf("Failed in SetCqlVersion: %s %s", ire.Why, t3e)
	}
	return nil
}

// If this connection has not yet successfully sent a "set keyspace" command to the server,
// do it now, returning an error if unsuccessful.
func (conn *CassandraConnection) RequireKeyspaceSet() error {
	if !conn.hasSetKeyspace {
		ire, err := conn.Client.SetKeyspace(conn.Keyspace)
		if err != nil || ire != nil {
			return fmt.Errorf("Couldn't set keyspace as required: %s %s", ire, err)
		}
		conn.hasSetKeyspace = true
	}
	return nil
}

// closes this connection
func (conn *CassandraConnection) Close() {
	if conn.Client != nil {
		conn.Client.Transport.Close()
	}
	conn.Client = nil
}

func (conn *CassandraConnection) Checkin() {
	conn.pool <- conn
}

/**
 *  Get columns from cassandra for current keyspace
 */
func (c *CassandraConnection) columns() (columns *[]cassandra.CfDef) {

	//DescribeKeyspace(keyspace string) (retval466 *KsDef, nfe *NotFoundException, ire *InvalidRequestException, err error)
	ksdef, _, _, err := c.Client.DescribeKeyspace(c.Keyspace)
	if err != nil {
		Log(ERROR, "error getting keyspace", err)
	}

	//for some reason, one of the CfDef cols is nil?    not sure why
	if ksdef.CfDefs.Len() > 1 {
		var col *cassandra.CfDef
		var idelta int = 0
		l := ksdef.CfDefs.Len() - 1
		cols := make([]cassandra.CfDef, l)
		columns = &cols
		for i := 0; i < ksdef.CfDefs.Len(); i++ {
			if ksdef.CfDefs.At(i) != nil {
				col = ksdef.CfDefs.At(i).(*cassandra.CfDef)
				cols[i-idelta] = *col
			} else {
				idelta++
			}
		}
	}
	return
}

func (c *CassandraConnection) ColumnsString() (out string) {

	out = fmt.Sprintf("=======   Cassandra Column Info for Keyspace %s  ===========\n", c.Keyspace)
	out += fmt.Sprintf("COLUMN \t\tColumnType\n")

	columns := c.columns()
	for i := 0; i < len((*columns)); i++ {
		out += fmt.Sprintf("%s \t\t %s\n", (*columns)[i].Name, (*columns)[i].ColumnType)
	}

	return
}

// Create a keyspace, with ReplicationFactor *repfactor*
func (c *CassandraConnection) CreateKeyspace(ks string, repfactor int) error {

	_, err := c.QueryNokeyspace(fmt.Sprintf(`CREATE KEYSPACE %s 
		WITH replication = { 'class' : 'SimpleStrategy',  'replication_factor' : %d };`, ks, repfactor))
	if err != nil {
		Log(ERROR, "Create Keyspace failed %s ", err)
		return err
	}
	ire, err := c.Client.SetKeyspace(ks)
	if ire != nil || err != nil {
		Log(ERROR, ire, err.Error())
		return err
	}
	return nil
}

func (c *CassandraConnection) DeleteKeyspace(ks string) string {

	//SystemDropKeyspace(keyspace string) 
	//(retval471 string, ire *InvalidRequestException, sde *SchemaDisagreementException, err error)
	ret, ire, sde, err := c.Client.SystemDropKeyspace(ks)
	if ire != nil || sde != nil || err != nil {
		Log(ERROR, "Keyspace deletion Error, possibly didn't exist? ", ire, sde, err)
	} else {
		Log(DEBUG, "Deleted Keyspace ", ks, ret)
	}

	return ret
}

func (c *CassandraConnection) CreateCounterCF(cf string) string {
	return c.createCF(cf, "UTF8Type", "CounterColumnType", "UTF8Type")
}

// create default column family, uses all "UTF8Type" for 
// validation, comparator, and key validation classes
func (c *CassandraConnection) CreateCF(cf string) string {
	return c.createCF(cf, "UTF8Type", "UTF8Type", "UTF8Type")
}

// Create column family with custom comparator/validators
//  
func (c *CassandraConnection) CreateColumnFamily(cf, comparator, validation, keyvalidation string) string {
	return c.createCF(cf, comparator, validation, keyvalidation)
}

// create column family, returns schemaid, if not successful
// then schemaid will be zero length indicating no change
func (c *CassandraConnection) createCF(cf, comparator, validation, keyvalidation string) string {

	cfdef := cassandra.NewCfDef()
	cfdef.Keyspace = c.Keyspace
	cfdef.Name = cf
	cfdef.ComparatorType = comparator
	cfdef.KeyValidationClass = keyvalidation
	cfdef.DefaultValidationClass = validation

	//    SystemAddColumnFamily(cf_def *CfDef) 
	//  (retval468 string, ire *InvalidRequestException, sde *SchemaDisagreementException, err error)
	ret, ire, sde, err := c.Client.SystemAddColumnFamily(cfdef)
	if ire != nil || sde != nil || err != nil {
		Log(ERROR, "an error occured", ire, sde, err)
	} else {
		Log(DEBUG, "Created Column Family ", cf, ret)
	}
	return ret
}

// drops a column family. returns the new schema id.
// if failure, zerolengh schema id indicating no change
func (c *CassandraConnection) DeleteCF(cf string) string {

	//SystemDropColumnFamily(column_family string) 
	//  (retval469 string, ire *InvalidRequestException, sde *SchemaDisagreementException, err error)
	ret, ire, sde, err := c.Client.SystemDropColumnFamily(cf)
	if ire != nil || sde != nil || err != nil {
		Log(ERROR, "an error occured", ire, sde, err)
	}
	return ret
}

// Mutate set of rows/cols for those rows
// Optional {@timestamp}, if you pass 0 it will create one for you
func (c *CassandraConnection) Mutate(cf string, mutateArgs map[string]map[string]string, timestamp int64) (err error) {

	if timestamp == 0 {
		timestamp = time.Now().UnixNano() / 1e6
	}

	//1:required map<binary, map<string, list<Mutation>>> mutation_map,
	mutateMap := makeMutationMap(cf, mutateArgs, timestamp)

	retry := func() error {

		//BatchMutate(mutation_map thrift.TMap, consistency_level ConsistencyLevel) 
		//  (ire *InvalidRequestException, ue *UnavailableException, te *TimedOutException, err error)
		ire, ue, te, err := c.Client.BatchMutate(mutateMap, cassandra.ConsistencyLevel_ONE)
		if ire != nil || ue != nil || te != nil || err != nil {
			errmsg := fmt.Sprint("BatchMutate error occured on ", cf, ire, ue, te, err)
			Log(ERROR, errmsg)
			return errors.New(errmsg)
		}
		return nil
	}

	return retryForClosedConn(c, retry)

}

// from map of (name/value) pairs create cassandra columns
func makeColumns(cols map[string]string, timestamp int64) []cassandra.Column {
	columns := make([]cassandra.Column, 0)
	for name, value := range cols {
		cassCol := NewColumn(name, value, timestamp)
		columns = append(columns, *cassCol)
	}
	return columns
}

// from nested map create mutation map thrift struct
func makeMutationMap(cf string, mutate map[string]map[string]string, timestamp int64) (mutationMap thrift.TMap) {

	mutationMap = thrift.NewTMap(thrift.STRING, thrift.MAP, len(mutate)) // len = # of keys(rows)

	for rowkey, cols := range mutate {

		colm := thrift.NewTMap(thrift.STRING, thrift.LIST, 1) // # of column families
		l := thrift.NewTList(thrift.STRUCT, 0)                // auto-expands, starting length should be 0
		columns := makeColumns(cols, timestamp)

		for i := 0; i < len(cols); i++ {
			cosc := cassandra.NewColumnOrSuperColumn()
			cosc.Column = &columns[i]
			mut := cassandra.NewMutation()
			mut.ColumnOrSupercolumn = cosc
			l.Push(mut)
		}

		// seems backwards, but.. row key on the outer map, cfname on inner
		colm.Set(cf, l)
		mutationMap.Set(rowkey, colm)
	}

	return
}

type Retryable func() error

func retryForClosedConn(c *CassandraConnection, retryfunc Retryable) error {

	err := retryfunc()

	if err != nil && strings.Contains(err.Error(), "Remote side has closed") {
		// Cannot read. Remote side has closed. Tried to read 4 bytes, but only got 0 bytes.
		Log(ERROR, "Trying to reopen for add/update ")
		c.Close()
		if err := c.Open(c.Keyspace); err != nil {
			return fmt.Errorf("Failed to reopen while retrying: %s", err.Error())
		}
		return retryfunc()
	}
	return err
}

// Insert Single row and column(s)
func (c *CassandraConnection) Insert(cf string, key string, cols map[string]string, timestamp int64) (err error) {

	if timestamp == 0 {
		timestamp = time.Now().UnixNano() / 1e6
	}

	if len(cols) > 1 {
		//1:required map<binary, map<string, list<Mutation>>> mutation_map,
		var mutateArgs map[string]map[string]string
		mutateArgs = make(map[string]map[string]string)
		mutateArgs[key] = cols
		mutateMap := makeMutationMap(cf, mutateArgs, timestamp)

		//BatchMutate(mutation_map thrift.TMap, consistency_level ConsistencyLevel) 
		//  (ire *InvalidRequestException, ue *UnavailableException, te *TimedOutException, err error)
		ire, ue, te, err := c.Client.BatchMutate(mutateMap, cassandra.ConsistencyLevel_ONE)
		if ire != nil || ue != nil || te != nil || err != nil {
			Log(ERROR, "an error occured on batch mutate", ire, ue, te, err)
		}

	} else {

		columns := makeColumns(cols, timestamp)
		cp := NewColumnParent(cf)

		retry := func() error {

			//Insert(key string, column_parent *ColumnParent, column *Column, consistency_level   ConsistencyLevel) 
			//  (ire *InvalidRequestException, ue *UnavailableException, te *TimedOutException, err error)
			ire, ue, te, err := c.Client.Insert([]byte(key), cp, &columns[0], cassandra.ConsistencyLevel_ONE)

			if ire != nil || ue != nil || te != nil || err != nil {
				errmsg := fmt.Sprint("Insert error occured on ", cf, " key ", key, ire, ue, te, err)
				Log(ERROR, errmsg)
				return errors.New(errmsg)
			}
			return nil
		}
		return retryForClosedConn(c, retry)

	}

	return
}

/**
 * Increment or decrement a counter.
 * 
 * @cfname string
 * @Key string
 * @Value (int32)
 */
func (c *CassandraConnection) Add(cf, key string, incr int64) error {

	//timestamp := time.Nanoseconds() / 1e6 
	cp := NewColumnParent(cf)
	counterCol := NewCounterColumn(key, incr)

	//Add(key string, column_parent *ColumnParent, column *CounterColumn, consistency_level ConsistencyLevel) 
	//    (ire *InvalidRequestException, ue *UnavailableException, te *TimedOutException, err error)
	ire, ue, te, err := c.Client.Add([]byte(key), cp, counterCol, cassandra.ConsistencyLevel_ONE)
	if ire != nil || ue != nil || te != nil || err != nil {
		Log(ERROR, "Counter Add Error, possibly column didn't exist? ", cf, " ", key, ire, ue, te, err)
		return nil // TODO, make error
	}
	return nil
}

type RetryableGet func() (interface{}, error)

func retryGetForClosedConn(c *CassandraConnection, retryfunc RetryableGet) (interface{}, error) {

	ret, err := retryfunc()

	if err != nil && strings.Contains(err.Error(), "Remote side has closed") {
		// Cannot read. Remote side has closed. Tried to read 4 bytes, but only got 0 bytes.
		Log(ERROR, "Trying to close/reopoen connection")
		c.Close()
		c.Open(c.Keyspace)
		return retryfunc()
	}
	return ret, err

}

func (c *CassandraConnection) get(rowkey string, cp *cassandra.ColumnPath) (cosc *cassandra.ColumnOrSuperColumn, err error) {

	//Get(key string, column_path *ColumnPath, consistency_level ConsistencyLevel) 
	//    (retval446 *ColumnOrSuperColumn, ire *InvalidRequestException, nfe *NotFoundException, ue *UnavailableException, te *TimedOutException, err error)
	ret, ire, nfe, ue, te, err := c.Client.Get([]byte(rowkey), cp, cassandra.ConsistencyLevel_ONE)
	if ire != nil || nfe != nil || ue != nil || te != nil || err != nil {
		errmsg := fmt.Sprint("Get Error, possibly column didn't exist? ", cp.ColumnFamily, rowkey, ire, ue, te, err)
		Log(ERROR, errmsg)

		// an error occured on batch mutate <nil> <nil> <nil> Cannot read. Remote side has closed. Tried to read 4 bytes, but only got 0 bytes.

		return ret, CassandraError(errmsg)
	}

	return ret, nil

}

/**
 * Get:  Gets Single Row-Col combo
 * 
 * @cfname string  Column Family name
 * @rowkey string  Row Key
 * @column string  Column name we want returned
 * 
 * returns *cassandra.Column
 */
func (c *CassandraConnection) Get(cfname, rowkey, col string) (cassCol *cassandra.Column, err error) {

	cp := NewColumnPath(cfname, col)
	cosc, _ := c.get(rowkey, cp)
	if cosc != nil && cosc.Column != nil {
		return cosc.Column, nil
	}
	return cassCol, CassandraError("No column returned")
}

/**
 * GetAll:  Gets Single Row record, columns
 * 
 * @cfname string  Column Family name
 * @rowkey string  Row Key
 * @reversed bool  start from last column
 * @colLimit int
 * 
 * returns []cassandra.Column
 */
func (c *CassandraConnection) GetAll(cf, rowkey string, reversed bool, colCt int) (cassCol []*cassandra.Column, err error) {

	return c.GetRange(cf, rowkey, "", "", reversed, colCt)

}

func (c *CassandraConnection) getslice(rowkey string, cp *cassandra.ColumnParent, sp *cassandra.SlicePredicate) (cassCol []*cassandra.Column, err error) {

	//GetSlice(key string, column_parent *ColumnParent, predicate *SlicePredicate, consistency_level ConsistencyLevel) 
	//  (retval447 thrift.TList, ire *InvalidRequestException, ue *UnavailableException, te *TimedOutException, err error)

	retry := func() (interface{}, error) {

		ret, ire, ue, te, err := c.Client.GetSlice([]byte(rowkey), cp, sp, cassandra.ConsistencyLevel_ONE)
		if ire != nil || ue != nil || te != nil || err != nil {
			err = CassandraError(fmt.Sprint("Error returned for Get ", ire, ue, te, err, c.Client))
			Log(ERROR, err.Error())
			return cassCol, err
		}
		cc := make([]*cassandra.Column, ret.Len())
		if ret != nil && ret.Len() > 0 {
			for i := 0; i < ret.Len(); i++ {
				cc[i] = (ret.At(i).(*cassandra.ColumnOrSuperColumn)).Column
			}
		}
		return cc, nil
	}
	retiface, err2 := retryGetForClosedConn(c, retry)
	if retiface != nil {
		return retiface.([]*cassandra.Column), err2
	}
	return nil, err2

}

func (c *CassandraConnection) GetCols(cf, rowkey string, cols []string) (cassCol []*cassandra.Column, err error) {

	sp := cassandra.NewSlicePredicate()
	cp := NewColumnParent(cf)
	//ColumnNames thrift.TList "column_names"; // 1
	l := thrift.NewTList(thrift.STRING, 0) // auto-expands, starting length should be 0

	for i := 0; i < len(cols); i++ {
		l.Push(cols[i])
	}
	sp.ColumnNames = l

	return c.getslice(rowkey, cp, sp)
}

/**
 * Get a single row's columns, which columns to get are defined by:
 * 
 * @param colfam  The name of the column family
 * @param rowkey   Row key
 * @param start. The column name to start the slice with. This attribute is not required, though there is no default value,
 *               and can be safely set to '', i.e., an empty byte array, to start with the first column name. Otherwise, it
 *               must a valid value under the rules of the Comparator defined for the given ColumnFamily.
 * @param finish. The column name to stop the slice at. This attribute is not required, though there is no default value,
 *                and can be safely set to an empty byte array to not stop until 'count' results are seen. Otherwise, it
 *                must also be a valid value to the ColumnFamily Comparator.
 * @param reversed. Whether the results should be ordered in reversed order. Similar to ORDER BY blah DESC in SQL.
 * @param count. How many columns to return. Similar to LIMIT in SQL. May be arbitrarily large, but Thrift will
 *               materialize the whole result into memory before returning it to the client, so be aware that you may
 *               be better served by iterating through slices by passing the last value of one call in as the 'start'
 *               of the next instead of increasing 'count' arbitrarily large.
 */
func (c *CassandraConnection) GetRange(cf, rowkey, start, finish string, reversed bool, colLimit int) (cassCol []*cassandra.Column, err error) {

	sp := cassandra.NewSlicePredicate()
	cp := NewColumnParent(cf)
	sp.SliceRange = NewSliceRange(start, finish, reversed, colLimit)

	return c.getslice(rowkey, cp, sp)

}

/**
 * Executes a CQL (Cassandra Query Language) statement and returns a * CqlResult containing the results.
 * 
 * Parameters:
 *  - Query
 *  - Compression  ["Compression_GZIP", "GZIP", "Compression_NONE", "NONE"]
 *  - ConsistencyLevel 
 *
 * conn.QueryArgs(`Create table user (...)`, "NONE", false, cassandra.ConsistencyLevel_QUORUM)
 */
func (c *CassandraConnection) QueryArgs(cql, compression string, requireKeyspace bool, consistency cassandra.ConsistencyLevel) (rows [][]*cassandra.Column, err error) {

	if c.Client == nil {
		Logf(ERROR, "Nil Client")
		return nil, errors.New("No Client for cass conn")
	}
	if requireKeyspace {
		if err := c.RequireKeyspaceSet(); err != nil {
			return nil, err
		}
	}
	Debug(cql)
	ret, ire, ue, te, sde, err := c.Client.ExecuteCql3Query([]byte(cql), cassandra.FromCompressionString(compression), consistency)
	if ire != nil || ue != nil || te != nil || sde != nil || err != nil {
		if err != nil && strings.Contains(err.Error(), "Remote side has closed") {
			// Cannot read. Remote side has closed. Tried to read 4 bytes, but only got 0 bytes.
			Log(ERROR, "Trying to reopen for add/update ")
			c.Close()
			if err := c.Open(c.Keyspace); err != nil {
				return nil, fmt.Errorf("Failed to reopen: %s", err.Error())
			}
			ret, ire, ue, te, sde, err = c.Client.ExecuteCql3Query([]byte(cql), cassandra.FromCompressionString(compression), consistency)
			if ire != nil || ue != nil || te != nil || sde != nil || err != nil {
				err = CassandraError(fmt.Sprint("Error on Query ", ire, ue, te, sde, err))
				Log(ERROR, err.Error())
				return rows, err
			}
		} else {
			err = CassandraError(fmt.Sprint("Error on Query ", ire, ue, te, sde, err, cql))
			Log(ERROR, err.Error())
			return rows, err
		}
	}
	if ret != nil && ret.Rows != nil {
		rowCt := ret.Rows.Len()
		//Debug("RowCt = ", rowCt)
		rows = make([][]*cassandra.Column, rowCt)
		var cqlRow cassandra.CqlRow
		//cols := make([]cassandra.Column, rowCt)
		for i := 0; i < rowCt; i++ {
			cr := (ret.Rows.At(i)).(*cassandra.CqlRow)
			cqlRow = *cr
			//Debug(*cr)
			//Debug(cr.Key)
			cols := make([]*cassandra.Column, cqlRow.Columns.Len())
			for x := 0; x < cqlRow.Columns.Len(); x++ {
				cols[x] = cqlRow.Columns.At(x).(*cassandra.Column)
				//cols[x] = cqlRow.Columns.At(x).(cassandra.Column)
			}
			rows[i] = cols
			//*(ret.At(i).(*cassandra.ColumnOrSuperColumn)).Column
			//cols[i] = (ret.Rows.At(i)).(cassandra.Column)
		}
		return rows, nil
	}
	return rows, err
}

/**
 * Executes a CQL (Cassandra Query Language) statement and returns a * CqlResult containing the results.
 * 
 * Parameters:
 *  - CQL Query
 */
func (c *CassandraConnection) Query(cql string) (rows [][]*cassandra.Column, err error) {
	return c.QueryArgs(cql, "NONE", true, cassandra.ConsistencyLevel_QUORUM)
}

/**
 * Executes a CQL (Cassandra Query Language) statement and returns a * CqlResult containing the results.
 *  Uses no keyspace (generaly create tables, truncate, etc)
 * 
 * Parameters:
 *  - CQL Query
 */
func (c *CassandraConnection) QueryNokeyspace(cql string) (rows [][]*cassandra.Column, err error) {
	return c.QueryArgs(cql, "NONE", false, cassandra.ConsistencyLevel_QUORUM)
}

/**
 * GetCounter:  gets the cassandra counter value for given column/key
 * 
 * @cfname string
 * @rowkey string
 * 
 * returns count (int64)
 */
func (c *CassandraConnection) GetCounter(cfname, rowkey string) (counter int64) {

	cp := NewColumnPath(cfname, rowkey)
	cosc, _ := c.get(rowkey, cp)
	if cosc != nil && cosc.CounterColumn != nil {
		return cosc.CounterColumn.Value
	}
	return counter
}

func NewColumnParent(name string) (c *cassandra.ColumnParent) {
	c = cassandra.NewColumnParent()
	c.ColumnFamily = name
	return
}

func NewColumnPath(cfname, colname string) (c *cassandra.ColumnPath) {
	c = cassandra.NewColumnPath()
	c.ColumnFamily = cfname
	c.Column = []byte(colname)
	return
}

func NewColumn(name string, value string, timestamp int64) (c *cassandra.Column) {
	c = cassandra.NewColumn()
	c.Name = []byte(name)
	c.Value = []byte(value)
	c.Timestamp = timestamp
	return
}

func NewCounterColumn(name string, value int64) (c *cassandra.CounterColumn) {
	c = cassandra.NewCounterColumn()
	c.Name = []byte(name)
	c.Value = value
	return
}

func NewSliceRange(start, finish string, reversed bool, limitCt int) (sr *cassandra.SliceRange) {
	sr = cassandra.NewSliceRange()
	sr.Start = []byte(start)
	sr.Finish = []byte(finish)
	sr.Reversed = reversed
	sr.Count = int32(limitCt)
	return
}

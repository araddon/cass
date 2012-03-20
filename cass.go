package cass

import (
  "errors"
  "fmt" 
  "log"
  "net"
  "strings"
  "sync"
  "github.com/araddon/cass/cassandra"
  "thrift"
  "time"
)


type CassandraError string

func (e CassandraError) Error() string   { return "cass error " + string(e) }

// The default logger is nil in Cassandra, and the default log level is
// 1= Error, to set them from within your app you set Logger, and LogLvel
//
//     func init()
//       // Logger is in your app
//       Logger =  log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
//       cass.Logger = Logger
//       cass.LogLevel = cass.DEBUG
//    }
//
var Logger *log.Logger = new(log.Logger)  

const (
  FATAL = 0
  ERROR = 1
  WARN  = 2
  INFO  = 3
  DEBUG = 4
)

var LogLevel int = ERROR

func Debug(v ...interface{}){
  doLog(fmt.Sprintln(v...), Logger)
}
func Debugf(format string, v ...interface{}){
  doLog(fmt.Sprintf(format, v...), Logger)
}
func Log(logLvl int, v ...interface{}) {
  if LogLevel >= logLvl {
    doLog(fmt.Sprintln(v...), Logger)
  }
}
func doLog(msg string, logger *log.Logger) {
  if logger != nil {
    Logger.Output(3, msg)
  }
}
func Logf(logLvl int, format string, v ...interface{}) {
  if LogLevel >= logLvl {
    doLog(fmt.Sprintf(format, v...), Logger)
  }
}


type CassandraConnection struct {
  Keyspace string
  Server string
  Id int
  pool chan *CassandraConnection
  Client *cassandra.CassandraClient
}

type KeyspaceConfig struct {
  servers []string
  MaxPoolSize int // Per server
  mu sync.Mutex
  //ServerCount int
  //next int  // next server
  Keyspace string
  pool chan *CassandraConnection
}

//type ConnMap map[string]chan *CassandraConnection
type KeyspaceConfigMap map[string]*KeyspaceConfig
var configMu sync.Mutex
var configMap = make(KeyspaceConfigMap)

//the default connection pool
//var connPool chan *CassandraConnection

func ConfigKeyspace (keyspace string, serverlist []string, poolsize int) *KeyspaceConfig {
  config := &KeyspaceConfig{servers:serverlist,Keyspace:keyspace,MaxPoolSize:poolsize}
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

  c.pool = make(chan *CassandraConnection, c.MaxPoolSize * len(c.servers))
  
  for i := 0; i < c.MaxPoolSize; i++ {
    for whichServer := 0; whichServer < len(c.servers) ; whichServer ++{
      // add empty values to the pool
      c.pool <- &CassandraConnection{Keyspace:c.Keyspace, Id:i, Server:c.servers[whichServer]}
    }
  }
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
        if conn.Client != nil && conn.Client.Transport != nil {
          conn.Client.Transport.Close()
        }
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

  return getConnFromPool(keyspace,keyspaceConfig.pool) 
}

func getConnFromPool(ks string, pool chan *CassandraConnection) (conn *CassandraConnection, err error) {

  conn = <-pool
  Logf(DEBUG, "in checkout, pulled off pool: remaining = %d, connid=%d Server=%s\n", len(pool), conn.Id, conn.Server)
  // BUG(ar):  an error occured on batch mutate <nil> <nil> <nil> Cannot read. Remote side has closed. Tried to read 4 bytes, but only got 0 bytes.
  if conn.Client == nil || conn.Client.Transport.IsOpen() == false {

    conn.pool = pool
    err = conn.Open(ks)
    Log(DEBUG, " in create conn, how is client? ", conn.Client, " is err? ", err)
    return conn, err
        
  } else if conn != nil && conn.Keyspace != ks {
    ire, er := conn.Client.SetKeyspace(ks)
    if ire != nil || er != nil {
      // most likely this is because it hasn't been created yet, so we will
      // ignore for now?
      Log(DEBUG, ire, er)
      //err = errors.New(fmt.Sprint(ire,er))
    }
  }
  return
}

// opens a cassandra connection
func (conn *CassandraConnection) Open(keyspace string) error {

  Log(DEBUG, "creating new cassandra connection ", conn.pool)
  tcpConn, er := net.Dial("tcp",conn.Server)
  if er != nil {
    return er
  }
  ts, err := thrift.NewTSocketConn(tcpConn)
  if err != nil {
    return err
  }

  if ts == nil {
    return errors.New("No TSocket connection?")
  }
  
  // the TSocket implements interface TTransport
  trans := thrift.NewTFramedTransport(ts)
  trans.Open()

  protocolfac := thrift.NewTBinaryProtocolFactoryDefault()

  conn.Client = cassandra.NewCassandraClientFactory(trans, protocolfac)

  Log(DEBUG, " in conn.Open, how is client? ", conn.Client)

  ire, er := conn.Client.SetKeyspace(keyspace)
  if ire != nil || er != nil {
    // most likely this is because it hasn't been created yet, so we will
    // ignore for now, as SetKeyspace() is purely optional
    Log(DEBUG, ire, er)
  }

  return nil
}

// closes this connection
func (conn *CassandraConnection) Close() {

  conn.Client = nil

}

func (conn *CassandraConnection) Checkin() {

  conn.pool <- conn
}

/**
 *  Get columns from cassandra for current keyspace
 */
func (c *CassandraConnection) columns() (columns *[]cassandra.CfDef){
  
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
    cols := make([]cassandra.CfDef,l)
    columns = &cols
    for i := 0; i < ksdef.CfDefs.Len(); i++ {
      if ksdef.CfDefs.At(i) != nil {
        col = ksdef.CfDefs.At(i).(*cassandra.CfDef)
        cols[i - idelta] = *col
      } else {
        idelta ++
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
    out += fmt.Sprintf("%s \t\t %s\n",(*columns)[i].Name, (*columns)[i].ColumnType)
  }

  return
}

func (c *CassandraConnection) CreateKeyspace(ks string,repfactor int) string {
    
  ksdef := cassandra.NewKsDef()
  ksdef.StrategyClass = "org.apache.cassandra.locator.SimpleStrategy"
  ksdef.Name = ks
  ksdef.ReplicationFactor = int32(repfactor)
  ksdef.CfDefs = thrift.NewTList(thrift.LIST, 0)// requires an empty cf def list
  //SystemAddKeyspace(ks_def *KsDef) 
  //  (retval470 string, ire *InvalidRequestException, sde *SchemaDisagreementException, err error)
  ret, ire, sde, err := c.Client.SystemAddKeyspace(ksdef)
  if ire != nil || sde != nil || err != nil {
    Log(ERROR, "Keyspace Creation Error ",ire,sde,err)
  } else {
    Log(DEBUG, "Created Keyspace ", ks,  ret)
    _, er := c.Client.SetKeyspace(c.Keyspace)
    if er != nil {
      Log(ERROR, "Error setting keyspace to ", ks, " ", er.Error())
    }
  }
  
  return ret
  
}

func (c *CassandraConnection) DeleteKeyspace(ks string) string {

  //SystemDropKeyspace(keyspace string) 
  //(retval471 string, ire *InvalidRequestException, sde *SchemaDisagreementException, err error)
  ret, ire, sde, err := c.Client.SystemDropKeyspace(ks)
  if ire != nil || sde != nil || err != nil {
    Log(ERROR, "Keyspace deletion Error, possibly didn't exist? ",ire,sde,err)
  } else {
    Log(DEBUG, "Deleted Keyspace ", ks,  ret)
  }
  
  return ret
}

func (c *CassandraConnection) CreateCounterCF(cf string) string {
  return c.createCF(cf,"UTF8Type","CounterColumnType","UTF8Type")
} 

// create default column family, uses all "UTF8Type" for 
// validation, comparator, and key validation classes
func (c *CassandraConnection) CreateCF(cf string) string {
  return c.createCF(cf,"UTF8Type","UTF8Type","UTF8Type")
} 

// Create column family with custom comparator/validators
//  
func (c *CassandraConnection) CreateColumnFamily(cf, comparator, validation, keyvalidation string) string {
  return c.createCF(cf,comparator,validation,keyvalidation)
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
    Log(ERROR,"an error occured",ire,sde,err)
  }
  Log(DEBUG, "Created Column Family ", cf , ret)
  return ret
}

// drops a column family. returns the new schema id.
// if failure, zerolengh schema id indicating no change
func (c *CassandraConnection) DeleteCF(cf string) string {
  
  //SystemDropColumnFamily(column_family string) 
  //  (retval469 string, ire *InvalidRequestException, sde *SchemaDisagreementException, err error)
  ret, ire, sde, err := c.Client.SystemDropColumnFamily(cf)
  if ire != nil || sde != nil || err != nil {
    Log(ERROR, "an error occured",ire,sde,err)
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
    ire, ue, te, err := c.Client.BatchMutate(mutateMap,cassandra.ONE)
    if ire != nil || ue != nil || te != nil || err != nil {
      errmsg := fmt.Sprint("BatchMutate error occured on ", cf ,ire,ue,te,err)
      Log(ERROR, errmsg)
      return errors.New(errmsg)
    }
    return nil
  }

  return retryForClosedConn(c,retry)

}

// from map of (name/value) pairs create cassandra columns
func makeColumns(cols map[string]string, timestamp int64) []cassandra.Column {
  columns := make([]cassandra.Column,0)
  for name, value := range cols {
    cassCol := NewColumn(name,value, timestamp)
    columns = append(columns,*cassCol)
  }
  return columns
}

// from nested map create mutation map thrift struct
func makeMutationMap(cf string, mutate map[string]map[string]string, timestamp int64) (mutationMap thrift.TMap) {
    
  mutationMap = thrift.NewTMap(thrift.STRING, thrift.MAP,len(mutate)) // len = # of keys(rows)
  
  for rowkey, cols := range mutate {

    colm := thrift.NewTMap(thrift.STRING, thrift.LIST,1) // # of column families
    l := thrift.NewTList(thrift.STRUCT, 0) // auto-expands, starting length should be 0
    columns := makeColumns(cols, timestamp)

    for i := 0; i < len(cols); i++ {
      cosc := cassandra.NewColumnOrSuperColumn()
      cosc.Column = &columns[i]
      mut := cassandra.NewMutation()
      mut.ColumnOrSupercolumn = cosc
      l.Push(mut)
    }

    // seems backwards, but.. row key on the outer map, cfname on inner
    colm.Set(cf,l)
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
    c.Open(c.Keyspace)
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
    ire, ue, te, err := c.Client.BatchMutate(mutateMap,cassandra.ONE)
    if ire != nil || ue != nil || te != nil || err != nil {
      Log(ERROR, "an error occured on batch mutate",ire,ue,te,err)
    }

  } else {
    
    columns := makeColumns(cols, timestamp)
    cp := NewColumnParent(cf)

    retry := func() error {

      //Insert(key string, column_parent *ColumnParent, column *Column, consistency_level   ConsistencyLevel) 
      //  (ire *InvalidRequestException, ue *UnavailableException, te *TimedOutException, err error)
      ire, ue, te, err := c.Client.Insert(key,cp,&columns[0],cassandra.ONE)

      if ire != nil || ue != nil || te != nil || err != nil {
        errmsg := fmt.Sprint("Insert error occured on ", cf, " key ", key,ire,ue,te,err)
        Log(ERROR, errmsg)
        return errors.New(errmsg)
      }
      return nil
    }
    return retryForClosedConn(c,retry)
    
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
func (c *CassandraConnection) Add(cf , key string, incr int64) error {

  //timestamp := time.Nanoseconds() / 1e6 
  cp := NewColumnParent(cf)
  counterCol := NewCounterColumn(key,incr)

  //Add(key string, column_parent *ColumnParent, column *CounterColumn, consistency_level ConsistencyLevel) 
  //    (ire *InvalidRequestException, ue *UnavailableException, te *TimedOutException, err error)
  ire, ue, te, err := c.Client.Add(key, cp, counterCol, cassandra.ONE)
  if ire != nil || ue != nil || te != nil || err != nil {
    Log(ERROR, "Counter Add Error, possibly column didn't exist? ", cf, " ", key ,ire,ue, te, err)
    return nil // TODO, make error
  }
  return nil
}


type RetryableGet func() (interface{},error)

func retryGetForClosedConn(c *CassandraConnection, retryfunc RetryableGet) (interface{},error) {
  
  ret, err := retryfunc()

  if err != nil && strings.Contains(err.Error(), "Remote side has closed") {
    // Cannot read. Remote side has closed. Tried to read 4 bytes, but only got 0 bytes.
    Log(ERROR,"Trying to close/reopoen connection")
    c.Close()
    c.Open(c.Keyspace)
    return retryfunc()
  }
  return ret, err

} 

func (c *CassandraConnection) get(rowkey string, cp *cassandra.ColumnPath) (cosc *cassandra.ColumnOrSuperColumn, err error) {
  
  //Get(key string, column_path *ColumnPath, consistency_level ConsistencyLevel) 
  //    (retval446 *ColumnOrSuperColumn, ire *InvalidRequestException, nfe *NotFoundException, ue *UnavailableException, te *TimedOutException, err error)
  ret, ire, nfe, ue, te, err := c.Client.Get(rowkey, cp, cassandra.ONE)
  if ire != nil || nfe!= nil || ue != nil || te != nil || err != nil {
    errmsg := fmt.Sprint("Get Error, possibly column didn't exist? ", cp.ColumnFamily, rowkey ,ire,ue, te, err)
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
func (c *CassandraConnection) Get(cfname , rowkey, col string) (cassCol *cassandra.Column, err error) {

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
func (c *CassandraConnection) GetAll(cf , rowkey string, reversed bool, colCt int) (cassCol *[]cassandra.Column, err error) {

  return c.GetRange(cf,rowkey, "", "", reversed, colCt)

}

func (c *CassandraConnection) getslice(rowkey string, cp *cassandra.ColumnParent, sp *cassandra.SlicePredicate) (cassCol *[]cassandra.Column, err error) {

  //GetSlice(key string, column_parent *ColumnParent, predicate *SlicePredicate, consistency_level ConsistencyLevel) 
  //  (retval447 thrift.TList, ire *InvalidRequestException, ue *UnavailableException, te *TimedOutException, err error)

  retry := func() (interface{},error) {

    ret, ire, ue, te, err := c.Client.GetSlice(rowkey, cp, sp, cassandra.ONE)
    if ire != nil || ue != nil || te != nil || err != nil {
      err = CassandraError(fmt.Sprint("Error returned for Get ", ire, ue, te, err, c.Client))
      Log(ERROR, err.Error())
      return cassCol, err
    }
    cc := make([]cassandra.Column,ret.Len())
    if ret != nil && ret.Len() > 0 {
      for i := 0; i < ret.Len(); i++ {
        cc[i] = *(ret.At(i).(*cassandra.ColumnOrSuperColumn)).Column
      }
    }
    return &cc, nil
  }
  retiface, err2 := retryGetForClosedConn(c,retry)
  if retiface != nil {
    return retiface.(*[]cassandra.Column), err2
  }
  return nil, err2

}

func (c *CassandraConnection) GetCols(cf, rowkey string, cols []string) (cassCol *[]cassandra.Column, err error) {

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
func (c *CassandraConnection) GetRange(cf, rowkey, start, finish string, reversed bool, colLimit int) (cassCol *[]cassandra.Column, err error) {

  sp := cassandra.NewSlicePredicate()
  cp := NewColumnParent(cf)
  sp.SliceRange = NewSliceRange(start,finish,reversed,colLimit)
  
  return c.getslice(rowkey, cp, sp)

}

/**
 * Executes a CQL (Cassandra Query Language) statement and returns a * CqlResult containing the results.
 * 
 * Parameters:
 *  - Query
 *  - Compression
 */
func (c *CassandraConnection) Query(cql, compression string) (rowMap map[string]*[]cassandra.Column, err error) {
    
  //ExecuteCqlQuery(query string, compression Compression) 
  //  (retval474 *CqlResult, ire *InvalidRequestException, ue *UnavailableException, te *TimedOutException, sde *SchemaDisagreementException, err error)

  ret, ire, ue, te, sde, err := c.Client.ExecuteCqlQuery(cql, cassandra.FromCompressionString(compression))
  if ire != nil || ue != nil || te != nil || sde != nil || err != nil {
    err = CassandraError(fmt.Sprint("Error on Query ", ire, ue, te, sde, err))
    Log(ERROR, err.Error())
    return rowMap, err
  }
  
  if ret != nil && ret.Rows != nil {
    rowCt := ret.Rows.Len()
    rowMap = make(map[string]*[]cassandra.Column, rowCt)
    var cqlRow cassandra.CqlRow
    //cols := make([]cassandra.Column, rowCt)
    for i := 0; i < rowCt; i++ {
      cr := (ret.Rows.At(i)).(*cassandra.CqlRow)
      cqlRow = *cr
      cols := make([]cassandra.Column,cqlRow.Columns.Len())
      for x := 0; x < cqlRow.Columns.Len(); x++ {
        cols[x] = *cqlRow.Columns.At(x).(*cassandra.Column)
      }
      rowMap[cqlRow.Key] = &cols
      //*(ret.At(i).(*cassandra.ColumnOrSuperColumn)).Column
      //cols[i] = (ret.Rows.At(i)).(cassandra.Column)
    }
    return rowMap, nil
  }
  return rowMap, err
}

/**
 * GetCounter:  gets the cassandra counter value for given column/key
 * 
 * @cfname string
 * @rowkey string
 * 
 * returns count (int64)
 */
func (c *CassandraConnection) GetCounter(cfname , rowkey string) (counter int64) {

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

func NewColumnPath(cfname , colname string) (c *cassandra.ColumnPath) {
  c = cassandra.NewColumnPath()
  c.ColumnFamily = cfname
  c.Column = colname
  return
}

func NewColumn(name string, value string, timestamp int64) (c *cassandra.Column) {
  c = cassandra.NewColumn()
  c.Name = name
  c.Value = value
  c.Timestamp = timestamp
  return
}

func NewCounterColumn(name string, value int64) (c *cassandra.CounterColumn) {
  c = cassandra.NewCounterColumn()
  c.Name = name
  c.Value = value
  return
}

func NewSliceRange(start, finish string, reversed bool, limitCt int) (sr *cassandra.SliceRange) {
  sr = cassandra.NewSliceRange()
  sr.Start = start
  sr.Finish = finish
  sr.Reversed = reversed
  sr.Count = int32(limitCt)
  return
}

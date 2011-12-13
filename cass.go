/*
  Cassandra Client.  The default behavior is a single connection pool
  for a single cassandra keyspace.   If this is not desired, and you have multiple
  keyspaces, then you need to implement a *GetConn* func for each keyspace as follows::

    // create  connection pool per keyspace:
    var keyspace1Pool chan CassandraConnection

    func GetKeyspace1Conn() (conn *CassandraConnection, err error) {
      return GetConnFromPool(&keyspace1Pool)
    }
*/
package cass

import (
  //"errors"
  "fmt" 
  "log"
  "os"
  "net"
  "thriftlib/cassandra"
  "thrift"
  "time"
)


var MaxPoolSize = 20 // Per server
var defaultAddr = "127.0.0.1:9160"
var cassClient *CassClient

type CassandraError string
func (e CassandraError) Error() string   { return "cass error " + string(e) }

type Compression int
const (
  GZIP Compression = 1
  NONE Compression = 2
)

type CLog struct {
  Logger *log.Logger
  LogLevel int  // 0 = fatal, 1 = error, 2 = warn, 3 = info, 4 = debug
}

var Logger = CLog{
  Logger: log.New(os.Stdout, "", log.Ldate|log.Ltime),
  LogLevel: 0,
}

func (c *CLog) Debug(v ...interface{}) {
  if c.LogLevel > 3 {
    c.Logger.Println(v...)
  }
}
func (c *CLog) Debugf(format string, v ...interface{}) {
  if c.LogLevel > 3 {
    c.Logger.Printf(format,v...)
  }
}
func (c *CLog) Info(v ...interface{}) {
  if c.LogLevel > 2 {
    c.Logger.Println(v...)
  }
}
func (c *CLog) Infof(format string, v ...interface{}) {
  if c.LogLevel > 2 {
    c.Logger.Printf(format,v...)
  }
}
func (c *CLog) Warn(v ...interface{}) {
  if c.LogLevel > 1 {
    c.Logger.Println(v...)
  }
}
func (c *CLog) Warnf(format string, v ...interface{}) {
  if c.LogLevel > 1 {
    c.Logger.Printf(format,v...)
  }
}
func (c *CLog) Error(v ...interface{}) {
  if c.LogLevel > 0 {
    c.Logger.Println(v...)
  }
}
func (c *CLog) Errorf(format string, v ...interface{}) {
  if c.LogLevel > 0 {
    c.Logger.Printf(format,v...)
  }
}
func (c *CLog) Fatal(v ...interface{}) {
  c.Logger.Println(v...)
}
func (c *CLog) Fatalf(format string, v ...interface{}) {
  c.Logger.Printf(format,v...)
}
type CassandraConnection struct {
  Keyspace string
  Server string
  Id int
  Client *cassandra.CassandraClient
}

type CassClient struct {
  servers *[]string
  ServerCount int
  next int  // next server
  Keyspace string
}


//the default connection pool
var connPool chan CassandraConnection

func GetClient (keyspace string, servers []string) *CassClient {
  cassClient = &CassClient{servers:&servers,Keyspace:keyspace,ServerCount:len(servers)}
  cassClient.initPool()
  return cassClient
}

func (c *CassClient) PoolSize() int {
  return len(connPool)
}

// clean up and close connections
func (c *CassClient) Close() {
  
  var conn CassandraConnection 
  if len(connPool) > 0 {
    for i := 0; i < len(connPool); i++ {
      conn = <-connPool
      if conn.Client != nil {
        conn.Client.Transport.Close()
      }
    }
  }
}

// initialize connections 
func (c *CassClient) initPool() {
  if connPool == nil || len(connPool) == 0 {
    connPool = make(chan CassandraConnection, MaxPoolSize * c.ServerCount)
    var  whichServer int = 0
    for i := 0; i < MaxPoolSize * c.ServerCount; i++ {
      // add empty values to the pool
      // TODO:  How come we can't add nil?
      connPool <- CassandraConnection{Keyspace:c.Keyspace, Id:i, Server:(*c.servers)[whichServer]}

      //TODO:  implement randomizer, round-robin etc
      whichServer ++
      if whichServer >= c.ServerCount {
        whichServer = 0
      }
    }
  } 
}

func GetCassConn(ks string) (conn *CassandraConnection, err error) {
  return GetConnFromPool(ks,&connPool) 
}

func GetConnFromPool(ks string, pool *chan CassandraConnection) (conn *CassandraConnection, err error) {
  var cn CassandraConnection
  
  cn = <-(*pool)
  conn = &cn
  Logger.Debugf("in checkout, pulled off pool remaining = %d, connid=%d Server=%s\n", len(connPool), cn.Id, cn.Server)

  if conn.Client == nil || conn.Client.Transport.IsOpen() == false {

    Logger.Debug("creating new cassandra connection ")
    ts, _ := cassClient.NewTSocket()
    if ts == nil {
      Logger.Fatal("NO Socket Connection", ts)
      return conn, nil
    }
    
    // the TSocket implements interface TTransport
    trans := thrift.NewTFramedTransport(ts)
    trans.Open()

    protocolfac := thrift.NewTBinaryProtocolFactoryDefault()

    conn.Client = cassandra.NewCassandraClientFactory(trans, protocolfac)

    ire, er := conn.Client.SetKeyspace(ks)
    if ire != nil || er != nil {
      // most likely this is because it hasn't been created yet, so we will
      // ignore for now?
      Logger.Debug(ire, er)
    }
    
  } else if conn != nil && conn.Keyspace != ks {
    ire, er := conn.Client.SetKeyspace(ks)
    if ire != nil || er != nil {
      // most likely this is because it hasn't been created yet, so we will
      // ignore for now?
      Logger.Debug(ire, er)
      //err = errors.New(fmt.Sprint(ire,er))
    }
  }
  return
}

func (c *CassClient) CheckoutConn() (conn *CassandraConnection, err error) {
  
  conn, err = GetCassConn(c.Keyspace)
  
  if err != nil {
    // most likely this is because it hasn't been created yet, so we will
    // ignore for now?
    Logger.Debug(err)
  }

  return
}

func (conn *CassandraConnection) Checkin() {

  connPool <- *conn
}

/**
 *  Get columns from cassandra for current keyspace
 */
func (c *CassandraConnection) columns() (columns *[]cassandra.CfDef){
  
  //DescribeKeyspace(keyspace string) (retval466 *KsDef, nfe *NotFoundException, ire *InvalidRequestException, err error)
  ksdef, _, _, err := c.Client.DescribeKeyspace(c.Keyspace)
  if err != nil {
    Logger.Error("error getting keyspace", err)
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
    Logger.Error("Keyspace Creation Error ",ire,sde,err)
  } else {
    Logger.Debug("Created Keyspace ", ks,  ret)
    _, er := c.Client.SetKeyspace(c.Keyspace)
    if er != nil {
      Logger.Error("Error setting keyspace to ", ks, " ", er.Error())
    }
  }
  
  return ret
  
}

func (c *CassandraConnection) DeleteKeyspace(ks string) string {

  //SystemDropKeyspace(keyspace string) 
  //(retval471 string, ire *InvalidRequestException, sde *SchemaDisagreementException, err error)
  ret, ire, sde, err := c.Client.SystemDropKeyspace(ks)
  if ire != nil || sde != nil || err != nil {
    Logger.Error("Keyspace deletion Error, possibly didn't exist? ",ire,sde,err)
  } else {
    Logger.Debug("Deleted Keyspace ", ks,  ret)
  }
  
  return ret
}

func (c *CassandraConnection) CreateCounterCF(cf string) string {
  return c.createCF(cf,"UTF8Type","CounterColumnType","UTF8Type")
} 

func (c *CassandraConnection) CreateCF(cf string) string {
  return c.createCF(cf,"UTF8Type","UTF8Type","UTF8Type")
} 

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
    Logger.Error("an error occured",ire,sde,err)
  }
  Logger.Debug("Created Column Family ", cf , ret)
  return ret
}

// drops a column family. returns the new schema id.
// if failure, zerolengh schema id indicating no change
func (c *CassandraConnection) DeleteCF(cf string) string {
  
  //SystemDropColumnFamily(column_family string) 
  //  (retval469 string, ire *InvalidRequestException, sde *SchemaDisagreementException, err error)
  ret, ire, sde, err := c.Client.SystemDropColumnFamily(cf)
  if ire != nil || sde != nil || err != nil {
    Logger.Error("an error occured",ire,sde,err)
  }
  return ret
}

func (c *CassandraConnection) Mutate(cf string, mutateArgs map[string]map[string]string) (err error) {

  timestamp := time.Now().UnixNano() / 1e6 
  

  //1:required map<binary, map<string, list<Mutation>>> mutation_map,
  mutateMap := makeMutationMap(cf, mutateArgs, timestamp)
    
  //BatchMutate(mutation_map thrift.TMap, consistency_level ConsistencyLevel) 
  //  (ire *InvalidRequestException, ue *UnavailableException, te *TimedOutException, err error)
  ire, ue, te, err := c.Client.BatchMutate(mutateMap,cassandra.ONE)
  if ire != nil || ue != nil || te != nil || err != nil {
    Logger.Error("an error occured on batch mutate",ire,ue,te,err)
  }

  return
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

/**
 *  Insert Single row and column(s)
 */
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
      Logger.Error("an error occured on batch mutate",ire,ue,te,err)
    }

  } else {
    
    //Insert(key string, column_parent *ColumnParent, column *Column, consistency_level   ConsistencyLevel) 
    //  (ire *InvalidRequestException, ue *UnavailableException, te *TimedOutException, err error)
    columns := makeColumns(cols, timestamp)
    cp := NewColumnParent(cf)

    ire, ue, te, err := c.Client.Insert(key,cp,&columns[0],cassandra.ONE)

    if ire != nil || ue != nil || te != nil || err != nil {
      Logger.Error("an error occured",ire,ue,te,err)
      return CassandraError("error" + fmt.Sprint(ire,ue,te,err))
    }
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
    Logger.Error("Counter Add Error, possibly column didn't exist? ", cf, " ", key ,ire,ue, te, err)
    return nil // TODO, make error
  }
  return nil
}

func (c *CassandraConnection) get(rowkey string, cp *cassandra.ColumnPath) (cosc *cassandra.ColumnOrSuperColumn, err error) {
  
  //Get(key string, column_path *ColumnPath, consistency_level ConsistencyLevel) 
  //    (retval446 *ColumnOrSuperColumn, ire *InvalidRequestException, nfe *NotFoundException, ue *UnavailableException, te *TimedOutException, err error)
  ret, ire, nfe, ue, te, err := c.Client.Get(rowkey, cp, cassandra.ONE)
  if ire != nil || nfe!= nil || ue != nil || te != nil || err != nil {
    Logger.Error("Get Column Error, possibly column didn't exist? ", cp.ColumnFamily, rowkey ,ire,ue, te, err)
    return ret, CassandraError("nothing returned for get ")
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
 * @colLimit int
 * 
 * returns []cassandra.Column
 */
func (c *CassandraConnection) GetAll(cf , rowkey string, colCt int) (cassCol *[]cassandra.Column, err error) {

  return c.GetRange(cf,rowkey, "", "", false, colCt)

}

func (c *CassandraConnection) getslice(rowkey string, cp *cassandra.ColumnParent, sp *cassandra.SlicePredicate) (cassCol *[]cassandra.Column, err error) {

  //GetSlice(key string, column_parent *ColumnParent, predicate *SlicePredicate, consistency_level ConsistencyLevel) 
  //  (retval447 thrift.TList, ire *InvalidRequestException, ue *UnavailableException, te *TimedOutException, err error)
  ret, ire, ue, te, err := c.Client.GetSlice(rowkey, cp, sp, cassandra.ONE)
  if ire != nil || ue != nil || te != nil || err != nil {
    err = CassandraError(fmt.Sprint("nothing returned for GetRange ", ire, ue, te, err))
    Logger.Error(err.Error())
    return cassCol, err
  }
  if ret != nil && ret.Len() > 0 {
    cc := make([]cassandra.Column,ret.Len())
    for i := 0; i < ret.Len(); i++ {
      cc[i] = *(ret.At(i).(*cassandra.ColumnOrSuperColumn)).Column
    }
    return &cc, nil
  }

  return cassCol, nil
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
    err = CassandraError(fmt.Sprint("nothing returned for Query ", ire, ue, te, sde, err))
    Logger.Error(err.Error())
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
  
// Get Thrift Socket Connection.  If list of servers is > 1 then round robins
// through list getting one for each.  Note, not the same as "GetConnection", once
// a connection is established it stays on same server so load balancing at the 
// checkout connection could potentially be more important?
func (c *CassClient) NewTSocket()  (*thrift.TSocket, thrift.TTransportException)  {
  Logger.Debug("cservers", c.servers, " ", c.next)
  server := (*c.servers)[c.next]
  srvCt := len((*c.servers))
  if srvCt > 1 && c.next + 1 < srvCt {
    c.next ++
  } else if srvCt > 1 && c.next + 1 == srvCt {
    c.next = 0
  } else {
    // ignore, len c.servers = 1
  }
  conn, er := net.Dial("tcp", server)
  if er != nil {
    return nil, nil
  }
  return thrift.NewTSocketConn(conn)
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

/*
Cassandra Client testing

>>>gotest -host 192.168.1.56:9160 

*/
package cass_test

import (
  "testing"
  //"fmt"
  //"os"
  "cass/cassandra"
  "flag"
  "strings"
  . "cass"
)

var conn *CassandraConnection
var ksConfig *KeyspaceConfig
var err error
var poolSize int
var cassServers string
var verbose bool 

func init() {
  flag.StringVar(&cassServers, "host", "127.0.0.1:9160", "Cassandra host/port combo")
  flag.IntVar(&poolSize, "poolsize", 20, "Default Pool Size = 20, change with this flag")
  LogLevel = 5
  flag.Parse()
  servers := strings.Split(cassServers,",")
  
  ConfigKeyspace("testing",servers, poolSize )

}

func cleanup() {
  _ = conn.DeleteKeyspace("testing")
  conn.Checkin()
  CloseAll()
}

func TestAllCassandra(t *testing.T) {

  initConn(t)
  defer cleanup()

  testConn(t)

  // setup testing keyspace
  testKeyspaceCrud(t)

  // first before others setup the CF 'testing' for crud tests
  testCFCrud(t)

  testInsertAndRead(t)

  testCounters(t)

  testMultiCrud(t)

  testByteType(t)

  testCQL(t)

}
func initConn(t *testing.T) {

  Log(DEBUG, "Connecting to Cassandra server: ", cassServers)

  conn, err = GetCassConn("testing")
  if err != nil || conn == nil || conn.Client == nil {
    t.Fatal("error on opening cassandra connection", err)
  }

}

// test if we have opened a connection to cassandra
func testConn(t *testing.T) {
  Log(DEBUG, "conn ok?", conn, conn.Client)
  if  conn.Client.Transport.IsOpen() != true {
    t.Errorf("error, no open connection")
  } 
  if ConnPoolSize("testing") != poolSize - 1 {
    t.Errorf("default pool size should be %d now that we have checked one out", poolSize - 1)
  }

  var conn2 *CassandraConnection
  conn2, err = GetCassConn("testing")
  if ConnPoolSize("testing")  != poolSize - 2 {
    t.Errorf("default pool size should be %d now that we have checked two out", poolSize - 2)
  }

  conn2.Checkin()
  if ConnPoolSize("testing")  != poolSize - 1 {
    t.Errorf("remaining pool size should be %d now that we have checked one back in", poolSize - 1)
  }
}

// test create of keyspace
func testKeyspaceCrud(t *testing.T) {

  ret := conn.CreateKeyspace("testing",1)

  if len(ret) < 10 {
    t.Errorf("error, create keyspace failed, if 'testing' keyspace exists, this will fail")
  } 
}

// test creation of column family, and delete
func testCFCrud(t *testing.T) {

  schemaid := conn.CreateCF("testing")
  if  len(schemaid) < 10 {
    t.Errorf("no valid schema id?")
  } 


  schemaid = conn.CreateCF("testing2")
  if  len(schemaid) < 10 {
    t.Errorf("no valid schema id? testing2 cf not created")
  } 
  // then delete
  schemadelid := conn.DeleteCF("testing2")
  // "schemaid" == "version" ?? if you don't get one, no change?
  if  len(schemadelid) < 10 {
    // ignore, just making sure it is not here, probably logged an error....
  } 

}

func testByteType(t *testing.T) {
  schemaid := conn.CreateColumnFamily("testing_byte_columns","BytesType","BytesType","UTF8Type")
  if  len(schemaid) < 10 {
    t.Errorf("no valid schema id?")
  } 
  col_val := []byte{0,255,1,99,134}
  var cols = map[string]string{
    string([]byte{0,255}): string(col_val),
  }

  err := conn.Insert("testing_byte_columns","bytescoltest",cols,0)
  if err != nil {
    t.Errorf("error, insert/read failed")
  } 

  col, _ := conn.Get("testing_byte_columns","bytescoltest",string([]byte{0,255}))
  new_col_val := []byte(col.Value)
  if col == nil ||  string(new_col_val) != string(col_val) {
    t.Errorf("get multi-col single row insert failed:  testing - keyinserttest2")
  }
}

// test insert, then read and verify
func testInsertAndRead(t *testing.T) {

  var cols = map[string]string{
    "lastnamet": "cassgo",
  }

  err := conn.Insert("testing","keyinserttest",cols,0)

  if err != nil {
    t.Errorf("error, insert/read failed")
  } 
  col, _ := conn.Get("testing","keyinserttest","lastnamet")
  if col == nil && col.Value != "cassgo" {
    t.Errorf("insert/get single row, single col failed: testing - keyinserttest")
  }

  // now multi-column single row insert
  cols2 := map[string]string{
    "lastnamet": "cassgo",
    "firstname": "go",
  }

  err2 := conn.Insert("testing","keyinserttest2",cols2,0)

  if err2 != nil {
    t.Errorf("error, insert/read failed on multicol insert")
  } 
  
  col, _ = conn.Get("testing","keyinserttest2","lastnamet")
  if col == nil || col.Value != "cassgo" {
    t.Errorf("get multi-col single row insert failed:  testing - keyinserttest2")
  }
}

// test creation, update, read of counter cf
func testCounters(t *testing.T) {

  schemaid := conn.CreateCounterCF("testct")
  if  len(schemaid) < 10 {
    t.Errorf("no valid schema id for counter?")
  } 

  err := conn.Add("testct","keyinserttest",int64(9))
  if err != nil {
    t.Errorf("error, insert/read failed")
  } 
  err = conn.Add("testct","keyinserttest",int64(10))
  if err != nil {
    t.Errorf("error, insert/read failed")
  } 

  //Get(cfname , rowkey, colname string) 
  ct := conn.GetCounter("testct","keyinserttest")

  if ct != int64(19) {
    t.Errorf("Crap, counter didn't work and equal 19", ct)
  }
}


// Test multi-col, & multi-row gets, updates
func testMultiCrud(t *testing.T) {

  var col cassandra.Column

  // now multi-column single row insert
  rows := map[string]map[string]string{
    "keyvalue1": map[string]string{"col1":"val1","col2": "val2","col3":"val3","col4":"val4"},
    "keyvalue2": map[string]string{"col1":"val1","col2": "val2"},
  }

  err := conn.Mutate("testing",rows,0)

  if err != nil {
    t.Errorf("error, insert/read failed on multicol insert")
  } 
  
  colget, _ := conn.Get("testing","keyvalue1","col1")
  if colget == nil || colget.Value != "val1" {
    t.Errorf("write Mutaet (multi-row, multi-col) failed for keyvalue1: col1 = val1")
  }

  // get all
  colsall, errall := conn.GetAll("testing","keyvalue1",false,1000)
  if colsall == nil || errall != nil {
    t.Error("GetAll failed with error or no response ", errall.Error())
  } else if len(*colsall) != 4{
    t.Errorf("GetAll failed expected 4 cols, got %d", len(*colsall))
  }

  // get range
  cols, err2 := conn.GetRange("testing","keyvalue1","col2","col3", false, 100)

  if err2 != nil {
    t.Errorf("GetRange failed by returning error %s", err2.Error())

  } else if cols == nil || len((*cols)) == 0 {
    t.Errorf("GetRange failed by returning empty")

  } else {

    col = (*cols)[0]
    if col.Value != "val2" || col.Name != "col2" {
      t.Errorf("GetRange failed with wrong val expected col2:val2 but was %s:%s", col.Name, col.Value)
    }
  }
  
  
  // get specific cols
  cols2, err3 := conn.GetCols("testing","keyvalue1",[]string{"col2","col4"})

  if err3 != nil {
    t.Errorf("GetCols failed by returning error %s", err3.Error())

  } else if cols2 == nil || len((*cols2)) == 0 {
    t.Errorf("GetCols failed by returning empty")

  } else {

    if len((*cols2)) != 2 {
      t.Errorf("GetCols failed by returning wrong col ct")
    } 
    col = (*cols2)[0]
    if col.Value != "val2" || col.Name != "col2" {
      t.Errorf("GetCols failed with wrong n/v expected col2:val2 but was %s:%s", col.Name, col.Value)
    }
    col = (*cols2)[1]
    if col.Value != "val4" || col.Name != "col4" {
      t.Errorf("GetCols failed with wrong n/v expected col4:val4 but was %s:%s", col.Name, col.Value)
    }
  }
  

}

// CQL (string queries) 
func testCQL(t *testing.T) {

  var col cassandra.Column

  _, err1 := conn.Query("INSERT INTO testing (KEY, col1,col2,col3,col4) VALUES('testingcqlinsert','val1','val2','val3','val4');", "NONE")
  if err1 != nil {
    t.Errorf("CQL Query Insert failed by returning error %s", err1.Error())
  } 



  //cqlsh> INSERT INTO users (KEY, password) VALUES ('jsmith', 'ch@ngem3a') USING TTL 86400;
  // cqlsh> SELECT * FROM users WHERE KEY='jsmith';
  // get cql query cols
  rows, err := conn.Query("SELECT col1,col2,col3,col4 FROM testing WHERE KEY='testingcqlinsert';", "NONE")
  Log(DEBUG, "Testing CQL:  SELECT * FROM testing WHERE KEY='testingcqlinsert';")

  if err != nil {
    t.Errorf("CQL Query failed by returning error %s", err.Error())

  } else if rows == nil || len(rows) == 0 {
    t.Errorf("Query failed by returning empty")

  } else {

    if len(rows) != 1 {
      t.Errorf("Query failed by returning wrong row ct")
    } 
    cols := rows["testingcqlinsert"]
    col = (*cols)[0]
    if len((*cols)) != 4 {
      t.Errorf("Query failed by returning wrong col ct")
    } 
    
    if col.Value != "val1" || col.Name != "col1" {
      t.Errorf("Query failed with wrong n/v expected col1:val1 but was %s:%s", col.Name, col.Value)
    }
    col3 := (*cols)[3]
    if col3.Value != "val4" || col3.Name != "col4" {
      t.Errorf("Query failed with wrong n/v expected col4:val4 but was %s:%s", col3.Name, col3.Value)
    }
  }
}



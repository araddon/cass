/*
Cassandra Client testing

>>>gotest -host 192.168.1.56:9160 -verbose=true

*/
package cass_test

import (
  "testing"
  //"fmt"
  "flag"
  . "cass"
)

var cassClient *CassClient
var conn *CassandraConnection
var err error
var defaultPoolSize int
var defaultHost string
var verbose bool 

func init() {
  flag.StringVar(&defaultHost, "host", "127.0.0.1:9160", "Cassandra host/port combo")
  flag.BoolVar(&verbose, "verbose", true, "Use verbose logging during test?")
  flag.Parse()
  if verbose {
    Logger.LogLevel = 5
  } else {
    Logger.LogLevel = 1
  }
  
}

func cleanup() {
  _ = conn.DeleteKeyspace("testing")
  cassClient.CheckinConn(conn)
  cassClient.Close()
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

}
func initConn(t *testing.T) {

  cassClient = NewCassandra("testing", []string{defaultHost})

  Logger.Debug("Connecting to Cassandra server: ", defaultHost)

  defaultPoolSize = MaxPoolSize // from cass
  if MaxPoolSize < 5 || cassClient.PoolSize() != defaultPoolSize {
    t.Errorf("default pool size should be %d", defaultPoolSize)
  }
  

  conn, err = cassClient.CheckoutConn()
  if err != nil {
    t.Errorf("error on opening cassandra connection")
  }
  
}

// test if we have opened a connection to cassandra
func testConn(t *testing.T) {
  if  conn.Client.Transport.IsOpen() != true {
    t.Errorf("error, no open connection")
  } 
  if cassClient.PoolSize() != defaultPoolSize - 1 {
    t.Errorf("default pool size should be %d now that we have checked one out", defaultPoolSize)
  }

  var conn2 *CassandraConnection
  conn2, err = cassClient.CheckoutConn()
  if cassClient.PoolSize() != defaultPoolSize - 2 {
    t.Errorf("default pool size should be %d now that we have checked two out", defaultPoolSize)
  }

  cassClient.CheckinConn(conn2)
  if cassClient.PoolSize() != defaultPoolSize - 1 {
    t.Errorf("default pool size should be %d now that we have checked one back in", defaultPoolSize)
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

func testMultiCrud(t *testing.T) {
  // now multi-column single row insert
  rows := map[string]map[string]string{
    "keyvalue1": map[string]string{"col1":"val1","col2": "val2"},
    "keyvalue2": map[string]string{"col1":"val1","col2": "val2"},
  }

  err := conn.Mutate("testing",rows)

  if err != nil {
    t.Errorf("error, insert/read failed on multicol insert")
  } 
  
  col, _ := conn.Get("testing","keyvalue1","col1")
  if col == nil || col.Value != "val1" {
    t.Errorf("write Mutaet (multi-row, multi-col) failed for keyvalue1: col1 = val1")
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
    t.Errorf("Crap, counter din't work and equal 19", ct)
  }
}
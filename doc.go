/*
Cass:  A simple Cassandra client in go.  Currently runs against cassandra 1.0.  

As of Nov 22-11:  Very early version, has very custom build chain (for now).   Built against Go Weekly so don't run *goinstall github.com/araddon/cass* unless you have a very recent goweekly/HEAD install.  It uses the new *errors* instead of *os.Error* and other changes.   Because of the Go changes it has custom `Thrift Go Lib <http://github.com/araddon/thrift4go>`_.   If you hack it needs a modified version of core `Thrift <http://github.com/araddon/thrift` in order to generate the /thrift/1.0/gen-go libs.  See commit log to see changes of the thrift or go4thrift changes.


Installation
=====================

First get a working version of `Thrift Go Lib <http://github.com/araddon/thrift4go>`_ ::

    goinstall github.com/araddon/thrift4go


then install the *thriftlib/cassandra*, then the *cass* client::
    
    goinstall github.com/araddon/cass/tree/master/thrift/1.0/gen-go/cassandra
    goinstall github.com/araddon/cass


Usage
====================================
Create a Connection, Keyspace, Column Family, Insert, Read::
    
    import "cass", "fmt"

    
    cassClient = cass.NewCassandra("testing", []string{"127.0.0.1:9160"})
    conn, _ = cass.GetCassConn("testing")

    defer func(){
      conn.Checkin()
      cassClient.Close()
    }

    // 1 is replication factor, will return err if already exist
    _ = conn.CreateKeyspace("testing",1)

    _ = conn.CreateCF("col_fam_name")

    var cols = map[string]string{
      "lastname": "golang",
    }

    // if you pass 0 timestamp it will generate one
    err := conn.Insert("col_fam_name","keyinserttest",cols,0)
    if err != nil {
      fmt.Println("error, insert/read failed")
    } 

    col, _ := conn.Get("col_fam_name","keyinserttest","lastname")
    if col == nil && col.Value != "cassgo" {
      fmt.Println("insert/get single row, single col failed: testcol - keyinserttest")
    }
    

Inserting more than one row::

    rows := map[string]map[string]string{
      "keyvalue1": map[string]string{"col1":"val1","col2": "val2"},
      "keyvalue2": map[string]string{"col1":"val1","col2": "val2"},
    }

    // 0 is the int64 NanoSecond timestamp, if you pass 0 it will generate
    err := conn.Mutate("col_fam_name",rows,0)

    // equivalent to
    err := conn.Mutate("col_fam_name",rows,time.Now().UnixNano())

    if err != nil {
      t.Errorf("error, insert/read failed on multicol insert")
    } 


Counter Columns::

    _ = conn.CreateCounterCF("testct")// testct is a column family name

    _ = conn.Add("testct","keyinserttest",int64(9))
    _ = conn.Add("testct","keyinserttest",int64(10))
     
    ct := conn.GetCounter("testct","keyinserttest")

    if ct != int64(19) {
      fmt.Println("Crap, counter didn't work and equal 19", ct)
    }


Get Many for column family, and row key specified return columns::

    // get all columns by all, all = ct specified
    // true = "reversed", start from last column
    colsall, errall := conn.GetAll("col_fam_name","keyvalue1", true,1000)

    // get Range (start/end) column comparator determines how start/end determined, also
    //   reversed (start at last row), and col limit ct
    cols, err2 := conn.GetRange("col_fam_name","keyvalue1","col2","col3", false, 100)

    // get specific cols
    cols2, err3 := conn.GetCols("col_fam_name","keyvalue1",[]string{"col2","col4"})
    

CQL::
    
  _, err1 := conn.Query("INSERT INTO col_fam_name (KEY, col1,col2,col3,col4) VALUES('testingcqlinsert','val1','val2','val3','val4');", "NONE")
  if err1 != nil {
    t.Errorf("CQL Query Insert failed by returning error %s", err1.Error())
  } 


  rows, err := conn.Query("SELECT col1,col2,col3,col4 FROM col_fam_name WHERE KEY='testingcqlinsert';", "NONE")
  cols := rows["testingcqlinsert"]
  if col.Value != "val1" || col.Name != "col1" {
    t.Errorf("Query failed with wrong n/v expected col1:val1 but was %s:%s", col.Name, col.Value)
  }


To Generate the Cassandra Go Thrift Client
===========================================

To generate from *cassandra.thrift*, you first need to have a working install of thrift.  Until changes make it into Thrift mainline you will need to use this modified version of thrift to support the newer Go Changes http://github.com/araddon/thrift .  This contains modifications to the go thrift compiler to allow compiling the cassandra.thrift::
    
    thrift --gen go cassandra.thrift     


*/
package cass
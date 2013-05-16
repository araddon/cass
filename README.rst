A simple Cassandra client in go.  Currently runs against cassandra 1.2.x

Alternate go clients to consider https://github.com/carloscm/gossie or https://github.com/tux21b/gocql instead.  Also a go cassandra connection pooling service https://github.com/samuraisam/cassbounce


Installation
=====================

First get a working version of `Thrift Go Lib <http://github.com/pomack/thrift4go>`_ .  ::
    
    go get github.com/pomack/thrift4go/lib/go/src/thrift

    # then install the go cass
    
    go get github.com/araddon/cass

Documentation
==================

See full doc here: http://gopkgdoc.appspot.com/pkg/github.com/araddon/cass


Usage
====================================
CQL::
    
  err, err1 := conn.Client.SetCqlVersion("3.0.0")
  if err != nil || err1 != nil {
    log.Println(err, err1)
  }

  res, err2 := conn.Query(`
    CREATE TABLE user (
      customerid int,
      uid int,
      gob blob,
      PRIMARY KEY (customerid, uid)
    );`)

  if err2 != nil {
    log.Println("CQL Query create failed by returning error ", err)
  }

  cql := fmt.Sprintf(`INSERT INTO user (customerid, uid, gob) VALUES (1234,128,'%#x');`, "gobdata")
  
  if _, err3 := conn.Query(cql, "NONE"); err3 != nil {
    log.Println("CQL Query Insert failed by returning error ", err3)
  } 


  rows, err4 := conn.Query("SELECT * FROM user WHERE customerid=1234;", "NONE")
  
  if len(rows) != 1 {
    log.Printf("Query failed was %v \n", rows)
  }

  

Thrift Interface
=============================

Create a Connection, Keyspace, Column Family, Insert, Read::
    
    import "github.com/araddon/cass", "fmt"
    func main() {

      cass.ConfigKeyspace("testing",[]string{"127.0.0.1:9160"}, 20 )
      conn, _ = cass.GetCassConn("testing")
      // since there is a pool of connections, you must checkin
      defer conn.Checkin()

      // 1 is replication factor, will return err if it already exists
      _ = conn.CreateKeyspace("testing",1)

      _ = conn.CreateCF("col_fam_name")

      // if you pass 0 timestamp it will generate one
      err := conn.Insert("col_fam_name","keyinserttest",map[string]string{"lastname": "golang"},0)
      if err != nil {
        fmt.Println("error, insert/read failed")
      } 

      col, _ := conn.Get("col_fam_name","keyinserttest","lastname")
      if col == nil && col.Value != "cassgo" {
        fmt.Println("insert/get single row, single col failed: testcol - keyinserttest")
      }
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
    



A simple Cassandra client in go.  Currently runs against cassandra 1.0.  

As of Nov 22-11:  Very early version, has very custom build chain (for now).   Built against Go Weekly so don't run *goinstall github.com/araddon/cass* unless you have a very recent goweekly/HEAD install.  It uses the new _errors_ instead of _os.Error_ and other changes.   Because of the Go changes it has custom `Thrift Go Lib <http://github.com/araddon/thrift4go>`.   If you hack it used a modified version of core `Thrift <http://github.com/araddon/thrift` in order to generate the /thrift/1.0/gen-go libs.  See commit log to see changes of the thrift or go4thrift changes.


Installation
=====================

First get a working version of `Thrift Go Lib <http://github.com/araddon/thrift4go>`::

    goinstall github.com/araddon/thrift4go


then install the *thriftlib/cassandra*, then the *cass* client::
    
    goinstall github.com/araddon/cass/tree/thrift/1.0/gen-go/cassandra
    goinstall github.com/araddon/cass


To Generate the Cassandra Go Thrift Client
===========================================

To generate from _cassandra.thrift_, you first need to have a working install of thrift.  Until changes make it into Thrift mainline you will need to use this modified version of thrift to support the newer Go Changes http://github.com/araddon/thrift .  This contains modifications to the go thrift compiler to allow compiling the cassandra.thrift::
    
    thrift --gen go cassandra.thrift     



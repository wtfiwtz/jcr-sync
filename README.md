# JCR Sync

This is a quick 'n' dirty Ruby / Rails script that helps to mirror data from one Java Content Repository (JCR) to another.

It is particularly helpful when the JCR XML import and exports fail, and no other options exist (e.g. underlying database is corrupt or bloated).

At this time, it supports most fields, _including date and binary fields_, but not JCR references.

It has been tested against Jackrabbit 2.10.1 and works with the `WebDAV` (HTTP) interface.

**NOTE: not yet tested against Jackrabbit Oak**

### Usage

Run something like this:

`WEBDAV_SRC=http://source-jcr.com:8080 WEBDAV_DST=http://target-jcr.com:8080 USERNAME=admin PASSWORD=pw ROOT=/jcrpath ruby test.rb`

### Sqlite3 Database

The Ruby script stores a running tally of synced data state in a Sqlite3 database. The script can be aborted and it will 
automatically start where it left off.
 
### Resyncing JCR nodes

There is a status column in the `nodes` database table that determines whether the node has finished syncing. This can 
be set to `:incomplete` (value = 1) to restart the sync for the node. 

There is also a `last_synced_at` database column but it is not currently used. It is an exercise for the reader to 
extend this script to re-start the sync to scan for node changes after a particular sync time (or alternatively, the 
start time of the script).

### Final Notes
 
I hope you find this useful.

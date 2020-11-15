
# Pentaho CouchBase Plugin

Jean-François Monteil
jfmonteil@gmail.com

Couchbase is an award-winning distributed NoSQL cloud database. It delivers unmatched versatility, performance, scalability, and financial value across cloud, on-premises, hybrid, distributed cloud, and edge computing deployments.

The package contains 1 step (more to come) :
* Couchbase Ouput : Writes to CouchBase key-value pairs.

Note that the value should be formated as JSON before the output step.

a number of sample transformations are included in the *sample* directory.

## Installation
In *delivery rep* you will find a zip that you can unzip in your *pentaho/design-tools/data-integration/plugin* folder.
Otherwise :  ``` mvn install ```

## Output step
The step is fully *Metadata Injection* compatible

![Output Step](https://github.com/jfmonteil/pentaho-influxdb-plugin/blob/master/screenshots/PentahoInfluxDBInputPlugin.png?raw=true)

### Step Name : Name of the step

### CouchBase Connection
Select Couchbase connection, create a new one *New Button*, or edit one *Edit Button*
The connection menu lets you specify host (or hosts), port, user, password and bucket. If no bucket is pecified, the default one will be used (covering thwhole cluster).

![Output Step](https://github.com/jfmonteil/pentaho-influxdb-plugin/blob/master/screenshots/PentahoInfluxDBInputPluginConnection.png?raw=true)

### Collection 
Lets you specify a collection (scope will come as well), if no colleciton is chosen. The default colleciton will be used covering the whole bucket.

### Insert type 
Lets you chose from (Upsert / Insert / Batch). Batch option is not implemented yet. Insert mode DOES NOT update a record if the key already exists. Upsert inserts or modifies existing records.



# Pentaho Couchbase Plugin

Jean-François Monteil
jfmonteil@gmail.com

Couchbase is an award-winning distributed NoSQL cloud database. It delivers unmatched versatility, performance, scalability, and financial value across cloud, on-premises, hybrid, distributed cloud, and edge computing deployments.

I added the first Pentaho / Kettle step which will let you load your Couchbase buckets.

The package contains 1 step (more to come) :
* Couchbase Ouptut : Writes to Couchbase key-value pairs.

A sample transformation is available in the *sample* directory

## Installation
In *delivery rep* you will find a zip that you can unzip in your *pentaho/design-tools/data-integration/plugin* folder.
Otherwise :  ``` mvn install ```

## Output step
The step is fully *Metadata Injection* compatible

![enter image description here](https://raw.githubusercontent.com/jfmonteil/PentahoCouchbasePlugin/main/screenshots/main_panel.png)

### Step Name : Name of the step

### Couchbase Connection
Select Couchbase connection, create a new one *New Button*, or edit one *Edit Button*
The connection menu lets you specify 
*host (or hosts), 
*port (unused for the moment)
*user
*password
*bucket. (If no bucket is specified, the default one will be used (covering the whole cluster).
Note that more advanced mechanisms of authentication and connection options (including cloud) are on my roadmap.
*Test Button lets you check the connection

![Couchbase connection tab](https://raw.githubusercontent.com/jfmonteil/PentahoCouchbasePlugin/main/screenshots/new_connection.png)
###Cloud
Select if it is a Couchbase DBAAS (Cloud) or "On premise". In this case the connection url will be *"cb.<your endpoint address>.dp.cloud.couchbase.com";*. Fill in your *<your endpoint address>* in the *host* field.

### Collection 
Lets you specify a collection (scope will come as well), if no collection is chosen. The default collection will be used covering the whole bucket. Collections and scope are not available before V6 of Couchbase.

### Insert type 
Lets you chose from (Upsert / Insert / Batch). 
* Batch option is not implemented yet (Roadmap : Assync loading)
* Insert mode DOES NOT update a record if the key already exists. 
* Upsert inserts or modifies existing records.

### Key
Input Field where the *key* value should be take from 

### Value
Input Field where the *Json Document* to insert along with the key. It Must be JSON formatted or it will blow at your face.
No Binary documents accepted yet, on the roadmap though
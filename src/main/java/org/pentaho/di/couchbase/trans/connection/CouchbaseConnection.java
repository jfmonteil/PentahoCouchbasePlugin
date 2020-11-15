package org.pentaho.di.couchbase.trans.connection;

import com.couchbase.client.java.*;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.json.*;
import com.couchbase.client.java.query.*;

//import okhttp3.OkHttpClient;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.BaseStep;

import java.io.IOException;
import java.util.Objects;
import java.time.Duration;

import org.pentaho.di.couchbase.trans.metastore.MetaStoreFactory;
import org.pentaho.di.couchbase.trans.connection.CouchbaseConnectionDialog;

@MetaStoreElementType(
  name = "Couchbase Connection",
  description = "This element describes how you can connect to Couchbase"
)
public class CouchbaseConnection extends Variables {

  private String name;

  @MetaStoreAttribute
  private String hostname;

  @MetaStoreAttribute
  private String port;

  @MetaStoreAttribute
  private String username;

  @MetaStoreAttribute(password = true)
  private String password;
  
  @MetaStoreAttribute
  private String bucketName;
  
  //private ResponseFormat responseFormat;
  
  private String apiUrlToUse;

  public CouchbaseConnection() {
    super();
  }

  public CouchbaseConnection( VariableSpace parent ) {
    super.initializeVariablesFrom( parent );
  }

  public CouchbaseConnection( VariableSpace parent, CouchbaseConnection source ) {
    super.initializeVariablesFrom( parent );
    this.name = source.name;
    this.hostname = source.hostname;
    this.port = source.port;
    this.username = source.username;
    this.password = source.password;
	//this.responseFormat=ResponseFormat.JSON;
	this.bucketName=source.bucketName;
  }

  public CouchbaseConnection( String name, String hostname, String port, String username, String password, String bucketName) {
    this.name = name;
    this.hostname = hostname;
    this.port = port;
    this.username = username;
    this.password = password;
	this.bucketName=bucketName;
	//this.responseFormat=ResponseFormat.JSON;
  }

  public Boolean test() throws KettleException {
    String apiUrlToUse="";
	String bucketName = this.getRealBucket();
	
	String username = this.getRealUsername();//"Administrator"
	String password = this.getRealPassword();//"password";
	Cluster cluster = null;
	Bucket bucket = null;
	Collection collection=null;
	Duration duration4 = Duration.ofSeconds(10);
      
	try {

		// Initialize thEncr.decryptPasswordOptionallyEncrypted(this.getRealPassword())e Connection
		cluster = Cluster.connect(this.getRealHostname(), username, password);
		cluster.waitUntilReady(duration4);
	//	logBasic("Connected to :"+this.getRealHostname()+" with "+ username +"pass"+ password);
		if(cluster==null){
			//logBasic("impossible to connect to cluster");
			throw new KettleException( "4 Error connecting to CouchBase impossible to connect to cluseter");
		}
		
		if(bucketName==null || bucketName.isEmpty())
		{
						throw new KettleException( "4 Error connecting to CouchBase Bucket not filled ");
						


			//logBasic("Bucket name not filled switching to basic");
			//bucket = cluster.openBucket();
		} else {
		bucket = cluster.bucket(bucketName);
		bucket.waitUntilReady(duration4);
		//logBasic("Opening Bucket:"+bucketName);
		}
	
		// get a collection reference
		collection = bucket.defaultCollection();
       if(collection==null){
			throw new KettleException( "4 Error connecting to CouchBase default collection=null");		 
			

			
		}
        /*QueryResult result = cluster.query("select \"Hello World\" as greeting");
        if(result==null){
			throw new KettleException( "4 Error connecting to CouchBase result=null");
		}*/
		
	} catch (Exception e ) {
		//System.out.println("Ooops!... something went wrong");
		//e.printStackTrace();
		 //logBasic("Exception connectiong to couchbase");
		 throw new KettleException( "Error connecting to Couchbase with user:"+ username +" password:"+ password+" bucket:"+bucketName+" host:"+this.getRealHostname(), e );
		 
		
	} finally {

		//Naive way to just close a single bucket
		//if(bucket != null) {
		//	bucket.close();
		//}

		// Disconnect and close all buckets
		if(cluster != null) {
			cluster.disconnect();
			
		}
		
	}
	return true;
  }
  
  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    CouchbaseConnection that = (CouchbaseConnection) o;
    return name.equals( that.name );
  }

  @Override public int hashCode() {
    return name == null ? super.hashCode() : name.hashCode();
  }

  @Override public String toString() {
    return name == null ? super.toString() : name;
  }

  public String getRealHostname() {
    return environmentSubstitute( hostname );
  }

  public String getRealPort() {
    return environmentSubstitute( port );
  }

  public String getRealUsername() {
    return environmentSubstitute( username );
  }
  
   public String getRealBucket() {
    return environmentSubstitute( bucketName );
  }
  


  public String getRealPassword() {
    return Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( password ) );
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets hostname
   *
   * @return value of hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @param hostname The hostname to set
   */
  public void setHostname( String hostname ) {
    this.hostname = hostname;
  }

  /**
   * Gets port
   *
   * @return value of port
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port The port to set
   */
  public void setPort( String port ) {
    this.port = port;
  }

  /**
   * Gets username
   *
   * @return value of username
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username The username to set
   */
  public void setUsername( String username ) {
    this.username = username;
  }

  /**
   * Gets password
   *
   * @return value of password
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password The password to set
   */
  public void setPassword( String password ) {
    this.password = password;
  }
  
    /**
   * Gets bucket
   *
   * @return value of bucket
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * @param  The bucket to set
   */
  public void setBucketName( String bucketName ) {
    this.bucketName = bucketName;
  }
  
  
  //connection to Couchbase.
  /*public static Couchbase connectToCouchbase() throws InterruptedException, IOException {
    return connectToCouchbase(null, null, ResponseFormat.JSON);
  }*/

  /*public static Couchbase connectToCouchbase(ResponseFormat responseFormat) throws InterruptedException, IOException {
    return connectToCouchbase(null, null, responseFormat);
  }*/
  /*public static Couchbase connectToCouchbase(String apiUrl) throws InterruptedException, IOException {
    return connectToCouchbase(new OkHttpClient.Builder(), apiUrl, ResponseFormat.JSON);
  }*/
  
  public Collection connectToCouchbase(String collectionName) throws InterruptedException, IOException {
    String bucketName = this.getRealBucket();
	Duration duration4 = Duration.ofSeconds(10);
	String username = this.getRealUsername();//"Administrator"
	String password = this.getRealPassword();//"password";
	Cluster cluster = null;
	Bucket bucket = null;
	Collection collection=null;
	try {

		// Initialize thEncr.decryptPasswordOptionallyEncrypted(this.getRealPassword())e Connection
		cluster = Cluster.connect(this.getRealHostname(), username, password);
		cluster.waitUntilReady(duration4);
		if(cluster==null){
			//logBasic("impossible to connect to cluster");
			throw new IOException( "3 Error connecting to CouchBase ");
		}
		
		if(bucketName==null || bucketName.isEmpty())
		{
			throw new IOException( "3 Error connecting to CouchBase Bucket not filled ");
			//logBasic("Bucket name not filled");
			//bucket = cluster.openBucket();
		} else {
		bucket = cluster.bucket(bucketName);
		bucket.waitUntilReady(duration4);
		//logBasic("Opeining Bucket:"+bucketName);
		}
	
		// get a collection reference
		collection = bucket.defaultCollection();
       if(collection==null){
			throw new IOException( "3 Error connecting to CouchBase default collection=null");
		}
        /*QueryResult result = cluster.query("select \"Hello World\" as greeting");
        if(result==null){
			throw new IOException( "3 Error connecting to CouchBase result=null");
		}*/
		
	} catch (Exception e ) {
		//System.out.println("Ooops!... something went wrong");
		//e.printStackTrace();
		 throw new IOException( "3 Error connecting to Couchbase ", e );
		
	}
	return collection;
  }
	
   public Collection connectToCouchbase() throws InterruptedException, IOException {
    String bucketName = this.getRealBucket();
	String username = this.getRealUsername();//"Administrator"
	String password = this.getRealPassword();//"password";
	Cluster cluster = null;
	Bucket bucket = null;
	Collection collection=null;
	Duration duration4 = Duration.ofSeconds(10);
	try {

		// Initialize thEncr.decryptPasswordOptionallyEncrypted(this.getRealPassword())e Connection
		cluster = Cluster.connect(this.getRealHostname(), username, password);
		cluster.waitUntilReady(duration4);
		if(cluster==null){
			//logBasic("impossible to connect to cluster");
			throw new IOException( "1 Error connecting to CouchBase no cluster ");
		}
		
		if(bucketName==null || bucketName.isEmpty())
		{
						throw new IOException( "1 Error connecting to CouchBase Bucket not filled ");

			//logBasic("Bucket name not filled");
			//bucket = cluster.openBucket();
		} else {
		bucket = cluster.bucket(bucketName);
		bucket.waitUntilReady(duration4);
		//logBasic("Opeining Bucket:"+bucketName);
		}
	
		// get a collection reference
		collection = bucket.defaultCollection();
       if(collection==null){
			throw new IOException( "1 Error connecting to CouchBase default collection=null");
		}
       /*QueryResult result = cluster.query("select \"Hello World\" as greeting");
        if(result==null){
			throw new IOException( "1 Error connecting to CouchBase result=null");
		}*/
		
	} catch (Exception e ) {
		//System.out.println("Ooops!... something went wrong");
		//e.printStackTrace();
		 throw new IOException( "1 Error connecting to Couchbase ", e );
		
	}
   
    return collection;
  }
  
   public Bucket connectToCouchbaseBucket() throws InterruptedException, IOException {
    String bucketName = this.getRealBucket();
	Duration duration4 = Duration.ofSeconds(10);
	String username = this.getRealUsername();//"Administrator"
	String password = this.getRealPassword();//"password";
	Cluster cluster = null;
	Bucket bucket = null;
	try {

		// Initialize thEncr.decryptPasswordOptionallyEncrypted(this.getRealPassword())e Connection
		cluster = Cluster.connect(this.getRealHostname(), username, password);
		cluster.waitUntilReady(duration4);
		if(cluster==null){
			//logBasic("impossible to connect to cluster");
			throw new IOException( "Error connecting to CouchBase ");
		}
		
		if(bucketName==null || bucketName.isEmpty())
		{
						throw new IOException( "2 Error connecting to CouchBase Bucket not filled ");

			//logBasic("Bucket name not filled");
			//bucket = cluster.openBucket();
		} else {
		bucket = cluster.bucket(bucketName);
	    bucket.waitUntilReady(duration4);
		//logBasic("Opeining Bucket:"+bucketName);
		}
	
		// get a collection reference
		
        if(bucket==null){
			throw new IOException( "2 Error connecting to CouchBase bucket=null");
		}
        
		
	} catch (Exception e ) {
		//System.out.println("Ooops!... something went wrong");
		//e.printStackTrace();
		 throw new IOException( "2 Error connecting to Couchbase ", e );
		
	}
   
    return bucket;
  }
  
    
  
}

package org.pentaho.di.couchbase.trans.steps.pentahocouchbase;

import java.util.List;

import org.pentaho.di.couchbase.trans.connection.CouchbaseConnection;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;

import com.couchbase.client.java.*;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.json.*;
import com.couchbase.client.java.query.*;

import java.io.InputStream;

public class PentahoCouchbasePluginOutputData extends BaseStepData implements StepDataInterface {

  public List<List<Object>> rows;
  
  public RowMetaInterface outputRowMeta;
  public CouchbaseConnection couchbaseConnection;
  public Collection collection;
  //public int[] fieldIndexes;
  //public String query;
  //public String accessToken;
  public int m_valueFieldNr;
  public int m_keyFieldNr;
  public String insertType;
  public IMetaStore metaStore;
  public int currentRow=0;

}
package org.pentaho.di.couchbase.trans.steps.pentahocouchbase;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import com.couchbase.client.java.*;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.json.*;
import com.couchbase.client.java.query.*;

import org.pentaho.di.couchbase.trans.steps.pentahocouchbase.PentahoCouchbasePluginOutputData;
//import org.pentaho.di.couchbase.trans.steps.pentahocouchbase.ReturnValue;

import java.util.ArrayList;
import java.util.List;

@Step(
  id = "PentahoCouchbasePluginOnput",
  name = "Couchbase Output",
  description = "Read data from couchbase",
  image = "PentahoCouchbasePluginOutput.svg",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Output"
)
@InjectionSupported( localizationPrefix = "couchbase.Injection.", groups = { "PARAMETERS", "RETURNS" } )
public class PentahoCouchbasePluginOutputMeta extends BaseStepMeta implements StepMetaInterface {

  public static Class<?> PKG = PentahoCouchbasePluginOutput.class;


  public static final String CONNECTION = "connection";
  public static final String COLLECTION = "collection";
  public static final String INSERTTYPE = "inserttype";
  public static final String KEY = "key";
  public static final String VALUE = "value";
  //public static final String QUERY = "query";
  //public static final String RETURNS = "returns";
  //public static final String VARIABLES = "variables";
 // public static final String RETURN = "return";
 // public static final String RETURN_NAME = "return_name";
 // public static final String RETURN_couchbase_NAME = "return_couchbase_name";
 // public static final String RETURN_TYPE = "return_type";
 // public static final String RETURN_LENGTH = "return_length";
//  public static final String RETURN_FORMAT = "return_format";
  

  @Injection( name = CONNECTION )
  private String connectionName;
  
  @Injection( name = COLLECTION )
  private String collection;
  
  @Injection( name = INSERTTYPE )
  private String inserttype;

  @Injection( name = KEY )
  private String key;
  
  @Injection( name = VALUE )
  private String value;

 // @InjectionDeep
 // private List<ReturnValue> returnValues;

  public PentahoCouchbasePluginOutputMeta() {
    super();
    //returnValues = new ArrayList<>();
  }

  @Override public void setDefault() {
    collection = "default";
	//variables=false;
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta, Trans trans ) {
    return new PentahoCouchbasePluginOutput( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new PentahoCouchbasePluginOutputData();
  }

  public String getDialogClassName() {
    return PentahoCouchbasePluginOutputDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface rowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
                                   Repository repository, IMetaStore metaStore ) throws KettleStepException {

   /* for ( ReturnValue returnValue : returnValues ) {
      try {
        int type = ValueMetaFactory.getIdForValueMeta( returnValue.getType() );
        ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta( returnValue.getName(), type );
        valueMeta.setLength( returnValue.getLength() );
		valueMeta.setConversionMask(returnValue.getFormat());
        valueMeta.setOrigin( name );
        rowMeta.addValueMeta( valueMeta );
      } catch ( KettlePluginException e ) {
        throw new KettleStepException( "Unknown data type '" + returnValue.getType() + "' for value named '" + returnValue.getName() + "'" );
      }
    }*/

  }

  @Override public String getXML() {
    StringBuilder xml = new StringBuilder();
    xml.append( XMLHandler.addTagValue( CONNECTION, connectionName ) );
	xml.append( XMLHandler.addTagValue( COLLECTION, collection ) );
	xml.append( XMLHandler.addTagValue( INSERTTYPE, inserttype ) );
	xml.append( XMLHandler.addTagValue( KEY, key ) );
	xml.append( XMLHandler.addTagValue( VALUE, value ) );
	//xml.append( "    " + XMLHandler.addTagValue( VARIABLES, variables ) );

   /* xml.append( XMLHandler.openTag( RETURNS ) );
    for ( ReturnValue returnValue : returnValues ) {
      xml.append( XMLHandler.openTag( RETURN ) );
      xml.append( XMLHandler.addTagValue( RETURN_NAME, returnValue.getName() ) );
      xml.append( XMLHandler.addTagValue( RETURN_couchbase_NAME, returnValue.getcouchbaseName() ) );
      xml.append( XMLHandler.addTagValue( RETURN_TYPE, returnValue.getType() ) );
      xml.append( XMLHandler.addTagValue( RETURN_LENGTH, returnValue.getLength() ) );
      xml.append( XMLHandler.addTagValue( RETURN_FORMAT, returnValue.getFormat() ) );
      xml.append( XMLHandler.closeTag( RETURN ) );
    }
    xml.append( XMLHandler.closeTag( RETURNS ) );*/

    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    connectionName = XMLHandler.getTagValue( stepnode, CONNECTION );
    collection = XMLHandler.getTagValue( stepnode, COLLECTION );
    inserttype = XMLHandler.getTagValue( stepnode, INSERTTYPE );
	key = XMLHandler.getTagValue( stepnode, KEY );
    value = XMLHandler.getTagValue( stepnode, VALUE );

    // Parse return values
    //
    /*Node returnsNode = XMLHandler.getSubNode( stepnode, RETURNS );
    List<Node> returnNodes = XMLHandler.getNodes( returnsNode, RETURN );
    returnValues = new ArrayList<>();
    for ( Node returnNode : returnNodes ) {
      String name = XMLHandler.getTagValue( returnNode, RETURN_NAME );
      String couchbaseName = XMLHandler.getTagValue( returnNode, RETURN_couchbase_NAME );
      String type = XMLHandler.getTagValue( returnNode, RETURN_TYPE );
      int length = Const.toInt(XMLHandler.getTagValue( returnNode, RETURN_LENGTH ), -1);
      String format = XMLHandler.getTagValue( returnNode, RETURN_FORMAT );
      returnValues.add( new ReturnValue( name, couchbaseName, type, length, format ) );
    }*/

    super.loadXML( stepnode, databases, metaStore );
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId ) throws KettleException {
    rep.saveStepAttribute( transformationId, stepId, CONNECTION, connectionName );
    rep.saveStepAttribute( transformationId, stepId, COLLECTION, collection );
	rep.saveStepAttribute( transformationId, stepId, INSERTTYPE, inserttype );
	rep.saveStepAttribute( transformationId, stepId, KEY, key );
	rep.saveStepAttribute( transformationId, stepId, VALUE, value );

	


   /* for ( int i = 0; i < returnValues.size(); i++ ) {
      ReturnValue returnValue = returnValues.get( i );
      rep.saveStepAttribute( transformationId, stepId, i, RETURN_NAME, returnValue.getName() );
      rep.saveStepAttribute( transformationId, stepId, i, RETURN_couchbase_NAME, returnValue.getcouchbaseName() );
      rep.saveStepAttribute( transformationId, stepId, i, RETURN_TYPE, returnValue.getType() );
      rep.saveStepAttribute( transformationId, stepId, i, RETURN_LENGTH, returnValue.getLength() );
      rep.saveStepAttribute( transformationId, stepId, i, RETURN_FORMAT, returnValue.getFormat() );
    }*/
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases ) throws KettleException {
    connectionName = rep.getStepAttributeString( stepId, CONNECTION );
    collection = rep.getStepAttributeString( stepId, COLLECTION );
	inserttype = rep.getStepAttributeString( stepId, INSERTTYPE );
    key = rep.getStepAttributeString( stepId, KEY );
    value = rep.getStepAttributeString( stepId, VALUE );
   
 /*   returnValues = new ArrayList<>();
    int nrReturns = rep.countNrStepAttributes( stepId, RETURN_NAME );
    for ( int i = 0; i < nrReturns; i++ ) {
      String name = rep.getStepAttributeString( stepId, i, RETURN_NAME );
      String couchbaseName = rep.getStepAttributeString( stepId, i, RETURN_couchbase_NAME );
      String type = rep.getStepAttributeString( stepId, i, RETURN_TYPE );
      int length = (int)rep.getStepAttributeInteger( stepId, i, RETURN_LENGTH );
      String format = rep.getStepAttributeString( stepId, i, RETURN_FORMAT );
      returnValues.add( new ReturnValue( name, couchbaseName, type, length, format) );
    }*/

  }

  /**
   * Gets connectionName
   *
   * @return value of connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /**
   * @param connectionName The connectionName to set
   */
  public void setConnectionName( String connectionName ) {
    this.connectionName = connectionName;
  }
  
    /**
   * Gets database
   *
   * @return value of database
   */
  public String getCollection() {
    return collection;
  }

  /**
   * @param database The database to set
   */
  public void setCollection( String collection ) {
    this.collection = collection;
  }

  /**
   * Gets query
   *
   * @return inserttype of inserttype
   */
  public String getInsertType() {
    return inserttype;
  }

  /**
   * @param inserttype The inserttype to set
   */
  public void setInsertType( String inserttype ) {
    this.inserttype = inserttype;
  }
  
  /**
   * @param key The key to set
   */  
  
  public String getKey() {
    return key;
  }
  
  /** 
  * @param key The key to set
  */
  public void setKey( String key ) {
    this.key = key;
  }

  /**
   * @param value The value to set
   */
  public void setValue( String value ) {
    this.value = value;
  }
  
    /**
   * @param value The value to set
   */  
  
  public String getValue() {
    return value;
  }


  /**
   * Gets returnValues
   *
   * @return value of returnValues
   
  public List<ReturnValue> getReturnValues() {
    return returnValues;
  }
  
   * @param returnValues The returnValues to set
  
  public void setReturnValues( List<ReturnValue> returnValues ) {
    this.returnValues = returnValues;
  }
  
  public boolean isVariables() {
    return variables;
  }
   
  public void setVariables( boolean variables ) {
    this.variables = variables;
  }**/
}



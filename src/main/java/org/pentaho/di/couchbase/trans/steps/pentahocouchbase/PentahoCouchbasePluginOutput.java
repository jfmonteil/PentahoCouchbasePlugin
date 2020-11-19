/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.pentaho.di.couchbase.trans.steps.pentahocouchbase;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.core.Const;


import org.pentaho.di.core.variables.Variables;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.sql.Timestamp;
import java.time.Instant;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import com.couchbase.client.java.*;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.json.*;
import com.couchbase.client.java.query.*;

/*import org.couchbase.Couchbase;
import org.couchbase.Couchbase.LogLevel;
import org.couchbase.Couchbase.ResponseFormat;
import org.couchbase.dto.BatchPoints;
import org.couchbase.dto.BoundParameterQuery.QueryBuilder;
import org.couchbase.dto.Point;
import org.couchbase.dto.Pong;
import org.couchbase.dto.Query;
import org.couchbase.dto.QueryResult;
import org.couchbase.dto.QueryResult.Series;
import org.couchbase.impl.CouchbaseImpl;*/

import org.pentaho.di.couchbase.trans.connection.CouchbaseConnection;
import org.pentaho.di.couchbase.trans.connection.CouchbaseConnectionUtil;
//import org.pentaho.di.couchbase.trans.steps.pentahocouchbase.ReturnValue;
import org.pentaho.di.couchbase.trans.metastore.MetaStoreFactory;
import org.pentaho.di.couchbase.trans.metastore.MetaStoreUtil;

import org.pentaho.di.couchbase.trans.steps.pentahocouchbase.PentahoCouchbasePluginOutputMeta;
import org.pentaho.di.couchbase.trans.steps.pentahocouchbase.PentahoCouchbasePluginOutputData;

/**
 * Describe your step plugin.
 * 
 */

public class PentahoCouchbasePluginOutput extends BaseStep implements StepInterface {

  private static Class<?> PKG = PentahoCouchbasePluginOutput.class; // for i18n purposes, needed by Translator2!!
  
  private PentahoCouchbasePluginOutputMeta meta;
  private PentahoCouchbasePluginOutputData data;
  
  public PentahoCouchbasePluginOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }
  
  /**
     * Initialize and do work where other steps need to wait for...
     *
     * @param stepMetaInterface
     *          The metadata to work with
     * @param stepDataInterface
     *          The data to initialize
     */
   @Override
   public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
        
		meta = (PentahoCouchbasePluginOutputMeta) smi;
        data = (PentahoCouchbasePluginOutputData) sdi;
        
		if ( StringUtils.isEmpty( meta.getConnectionName() ) ) {
		  log.logError( "You need to specify a Couchbase Connection connection to use in this step" );
		  return false;
		}
	    /*try {
   	   	   
		   
		} catch (Exception e) {
			logError("Exception",e.getMessage(),e);
		}*/
		
        
		if (super.init(smi, sdi)) {
            try {
                				
				//Sheets service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, PentahoCouchbasePluginCredentials.getCredentialsJson(scope,environmentSubstitute(meta.getJsonCredentialPath()))).setApplicationName(APPLICATION_NAME).build();
				//String range=environmentSubstitute(meta.getWorksheetId());
				//ValueRange response = service.spreadsheets().values().get(environmentSubstitute(meta.getSpreadsheetKey()),range).execute();             
			  data.metaStore = MetaStoreUtil.findMetaStore( this );
			  data.couchbaseConnection = CouchbaseConnectionUtil.getConnectionFactory(data.metaStore).loadElement(environmentSubstitute(meta.getConnectionName()));
			  data.couchbaseConnection.initializeVariablesFrom(this);		  
			  Collection collection=data.couchbaseConnection.connectToCouchbase();
			  if (collection==null)
			  {
				  logError("No collection");
				  return false;
			  }
			  data.collection=collection;
			 
			  //String query=environmentSubstitute(meta.getQuery());	
			 
			  //Couchbase.setDatabase(environmentSubstitute(meta.getDatabase()));
			 // QueryResult response = Couchbase.query(new Query(query));
		  //List<String> columns = result.getResults().get(0).getSeries().getColumns();
			/*  if(response==null) {
					logError("No response found for couchbase Database : "+environmentSubstitute( meta.getDatabase())+" for query :"+query);
			  } else 
					{				
					List<List<Object>> values = response.getResults().get(0).getSeries().get(0).getValues();
					logBasic("Reading Sheet, found: "+values.size()+" rows");
					if (values == null || values.isEmpty()) {
						logError("No response found for couchbase Database : "+environmentSubstitute( meta.getDatabase())+" for query :"+meta.getQuery());
						} else {
							data.rows=values;
						}
					}*/
				
            } catch (Exception e) {
                logError("Error: for couchbase Database : on  "+data.couchbaseConnection.getIsCloud()+" ,Collection :"+environmentSubstitute(meta.getCollection())+" for Bucket :"+data.couchbaseConnection.getRealBucket() +"exception"+ e.getMessage(), e);
                setErrors(1L);
                stopAll();
                return false;
            }

            return true;
        }
        return false;
    }
   

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    
	meta = (PentahoCouchbasePluginOutputMeta) smi;
    data = (PentahoCouchbasePluginOutputData) sdi;
	
	//RowMetaInterface inputRowMeta = getInputRowMeta();
	
	Object[] row = getRow();
	int numErrors = 0;
    if (first) {
		if(row==null){
			logError( BaseMessages.getString( PentahoCouchbasePluginOutputMeta.PKG, "PentahoCouchbasePluginOutput.Log.NoRow" ) ); //$NON-NLS-1$		
			return false;
	    }
		first = false;
	    logBasic("First");
		data.outputRowMeta=getInputRowMeta().clone();
		if(data.outputRowMeta==null){
		logError( BaseMessages.getString( PentahoCouchbasePluginOutputMeta.PKG, "PentahoCouchbasePluginOutput.Log.noinboundfields" ) ); //$NON-NLS-1$
			numErrors++;
	    }	
		meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);
		
		logBasic("Checking data : key : "+meta.getKey()+" value :"+meta.getValue());
		//Getting key and value
		String keyField=meta.getKey();
		String valueField=meta.getValue();
		
		if ( Const.isEmpty( keyField ) || Const.isEmpty( valueField ) ) {
			logError( BaseMessages.getString( PentahoCouchbasePluginOutputMeta.PKG, "PentahoCouchbasePluginOutput.Log.FieldNameIsNull" ) ); //$NON-NLS-1$
			numErrors++;
	    }
		data.m_valueFieldNr = data.outputRowMeta.indexOfValue( valueField );
	    data.m_keyFieldNr = data.outputRowMeta.indexOfValue( keyField );
		if ( data.m_valueFieldNr < 0 || data.m_keyFieldNr<0) {
        logError( BaseMessages
            .getString( PentahoCouchbasePluginOutputMeta.PKG, "PentahoCouchbasePluginOutput.Log.CouldntFindField", keyField +" "+valueField) ); //$NON-NLS-1$
        numErrors++;
		if ( numErrors > 0 ) {
			logError("too many errors");
			setErrors( numErrors );
			stopAll();
        return false;
        }
      }
	}
    if(!first && row==null){
			logBasic( BaseMessages.getString( PentahoCouchbasePluginOutputMeta.PKG, "PentahoCouchbasePluginOutput.Log.NoMoreRows" ) ); //$NON-NLS-1$		
			setOutputDone();
			return false;
	   }
     
	  //logDetail("row :"+data.currentRow);
	  //logBasic("Insert type"+meta.getInsertType());
	  try{
		MutationResult result = null;		
		String rawValue = row[data.m_valueFieldNr].toString();
		String rawKey = row[data.m_keyFieldNr].toString();
		//logDetail("data inbound value:"+rawValue+ "for key:"+rawKey+" Optype="+meta.getInsertType());
		//String val=JSON.stringify(rawValue);
		JsonObject content=JsonObject.fromJson(rawValue);
		if(meta.getInsertType()=="Upsert"){
			 //logDetail("Upsert");
			 try{

			 result = data.collection.upsert(rawKey, content);
			 } catch (Exception ex) {
				logError("Upsert Failed ! "+ex);
				return false;
						
				}
		}
		if(meta.getInsertType()=="Insert"){
			 //logDetail("Insert");

			 try{
			 result = data.collection.insert(rawKey, content);
			 } catch (Exception ex) {
				logError("Inseret failed"+ex);
				return false;
						
				}
		}
		else{
			
			 //logDetail("Upsert by default");
			 try{

			 result = data.collection.upsert(rawKey, content);
			 } catch (Exception ex) {
				logError("Upsert by default Failed ! "+ex);
				return false;
						
			 }
		}
        //byte[] message = messageToBytes( rawMessage, data.m_inputFieldMeta );  
	  data.currentRow++; 
	  } catch (Exception e ) {
      if ( !getStepMeta().isDoingErrorHandling() ) {
        logError(
            BaseMessages.getString( PentahoCouchbasePluginOutputMeta.PKG, "PentahoCouchbasePluginOutput.ErrorInStepRunning", e.getMessage() ) );
        setErrors( 1 );
        stopAll();
        setOutputDone();
		putError( getInputRowMeta(), row, 1, e.toString(), null, getStepname() );
        return false;
      }
      
	
    }
    
    return true;
  }
   private Object getRowDataValue(final ValueMetaInterface targetValueMeta, final ValueMetaInterface sourceValueMeta, final Object value, final DateFormat df,final DateTimeFormatter f) throws KettleException
    {
        if (value == null) {
            return value;
        }

        if (ValueMetaInterface.TYPE_TIMESTAMP  == targetValueMeta.getType()) {
            //Class.isAssignableFrom(Class)
			try{
			//logBasic("This is a Timestamp (type:"+sourceValueMeta.getType()+")conversion Converting :"+value.toString()+" with mask:"+sourceValueMeta.getConversionMask());

			LocalDateTime localDateTime = LocalDateTime.from(f.parse(value.toString()));
			Timestamp timestamp = Timestamp.valueOf(localDateTime);
			return targetValueMeta.convertData(sourceValueMeta, timestamp);

			
			} catch (ClassCastException exc) {
            logError("Timestamp class cast exeption");
            return targetValueMeta.convertData(sourceValueMeta, value.toString());			
			}
        }
		if (ValueMetaInterface.TYPE_STRING == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, value.toString());
        }
        
        if (ValueMetaInterface.TYPE_NUMBER == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, Double.valueOf(value.toString()));
        }
        
        if (ValueMetaInterface.TYPE_INTEGER == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, Long.valueOf(value.toString()));
        }
        
        if (ValueMetaInterface.TYPE_BIGNUMBER == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, new BigDecimal(value.toString()));
        }
        
        if (ValueMetaInterface.TYPE_BOOLEAN == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, Boolean.valueOf(value.toString()));
        }
        
        if (ValueMetaInterface.TYPE_BINARY == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, value);
        }

        if (ValueMetaInterface.TYPE_DATE == targetValueMeta.getType()) {
            try {
                return targetValueMeta.convertData(sourceValueMeta, df.parse(value.toString()));
            } catch (final ParseException e) {
                throw new KettleValueException("Unable to convert data type of value");
            }
        }

        throw new KettleValueException("Unable to convert data type of value");
    }
  
   private Object[] readRow() {
        try {
            logRowlevel("Allocating :" + Integer.toString(data.outputRowMeta.size()));
			Object[] outputRowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
            int outputIndex = 0;
		    int logcur=data.currentRow;			
			logRowlevel("Reading Row: "+Integer.toString(data.currentRow)+" out of : "+Integer.toString(data.rows.size()));         
			if (data.currentRow < data.rows.size()) {
                List<Object> row= data.rows.get(data.currentRow);
                for (ValueMetaInterface column : data.outputRowMeta.getValueMetaList()) {
                Object value=null;				
				logRowlevel("Reading columns: "+Integer.toString(outputIndex)+" out of : "+Integer.toString(row.size()));
				if(outputIndex>row.size()-1){
				  logRowlevel("Beyond size"); 
				  outputRowData[outputIndex++] = null;
				}
				else {	
						if(row.get(outputIndex)!=null){
							logRowlevel("getting value" +Integer.toString(outputIndex));
							value = row.get(outputIndex);
							logRowlevel("got value "+Integer.toString(outputIndex));

						}
						if (value == null)
						{
							//||value.isEmpty()||value==""
							outputRowData[outputIndex++] = null;
							logRowlevel("null value");
						}
						else {
							DateFormat df= (column.getType() == ValueMetaInterface.TYPE_DATE)? new SimpleDateFormat(column.getConversionMask()): null;
							DateTimeFormatter f = (column.getType()==ValueMetaInterface.TYPE_TIMESTAMP)? DateTimeFormatter.ofPattern(column.getConversionMask()):null;
							outputRowData[outputIndex++] = getRowDataValue(column,column,value,df,f);
							logRowlevel("value : "+value.toString());
						}
					 }
                }
            } else {
                logBasic("Finished reading last row "+ Integer.toString(data.currentRow) +" / "+Integer.toString(data.rows.size()));
				return null;
            }
            return outputRowData;
        } catch (Exception e) {
            logError("Exception reading value :" +e.getMessage());
			return null;
        }
    }
}
package org.pentaho.di.couchbase.trans.steps.pentahocouchbase;


import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;



import org.pentaho.di.couchbase.trans.connection.CouchbaseConnection;
import org.pentaho.di.couchbase.trans.connection.CouchbaseConnectionUtil;
//import org.pentaho.di.couchbase.trans.steps.pentahocouchbase.ReturnValue;
import org.pentaho.di.couchbase.trans.metastore.MetaStoreFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;

import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.ComboValuesSelectionListener;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.Props;


import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import com.couchbase.client.java.*;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.json.*;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.manager.collection.CollectionSpec;


import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;

public class PentahoCouchbasePluginOutputDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = PentahoCouchbasePluginOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wStepname;
  
  private CCombo wCollection;
  //private Text wInsertType;
  //private Text wInsertBucket;
  private CCombo wConnection;
  private CCombo wInsertType;
  private CCombo wKey;
  private CCombo wValue;
 // private StyledTextComp wQuery;
   private TableView wValueFieds;
  //private Label wlVariables;
  //private Button wVariables;
  //private FormData fdlVariables, fdVariables;

  //private TableView wReturns;

  private PentahoCouchbasePluginOutputMeta input;
  
 // private Label wlPosition;
 // private FormData fdlPosition;
  private boolean gotPreviousFields = false;


  public PentahoCouchbasePluginOutputDialog( Shell parent, Object inputMetadata, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta) inputMetadata, transMeta, stepname );
    input = (PentahoCouchbasePluginOutputMeta) inputMetadata;

    // Hack the metastore...
    //
    metaStore = Spoon.getInstance().getMetaStore();
  }

  @Override public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    FormLayout shellLayout = new FormLayout();
    shell.setLayout( shellLayout );
    shell.setText( "Couchbase Output" );
			
    ModifyListener lsMod = new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                input.setChanged();
            }
    };
    changed = input.hasChanged();

    ScrolledComposite wScrolledComposite = new ScrolledComposite( shell, SWT.V_SCROLL | SWT.H_SCROLL );
    FormLayout scFormLayout = new FormLayout();
    wScrolledComposite.setLayout( scFormLayout );
    FormData fdSComposite = new FormData();
    fdSComposite.left = new FormAttachment( 0, 0 );
    fdSComposite.right = new FormAttachment( 100, 0 );
    fdSComposite.top = new FormAttachment( 0, 0 );
    fdSComposite.bottom = new FormAttachment( 100, 0 );
    wScrolledComposite.setLayoutData( fdSComposite );

    Composite wComposite = new Composite( wScrolledComposite, SWT.NONE );
    props.setLook( wComposite );
    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment( 0, 0 );
    fdComposite.right = new FormAttachment( 100, 0 );
    fdComposite.top = new FormAttachment( 0, 0 );
    fdComposite.bottom = new FormAttachment( 100, 0 );
    wComposite.setLayoutData( fdComposite );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    wComposite.setLayout( formLayout );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Step name line
    //
    Label wlStepname = new Label( wComposite, SWT.RIGHT );
    wlStepname.setText( "Step name" );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( wlStepname, 0, SWT.CENTER );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;


    Label wlConnection = new Label( wComposite, SWT.RIGHT );
    wlConnection.setText( "Couchbase Connection" );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormData();
    fdlConnection.left = new FormAttachment( 0, 0 );
    fdlConnection.right = new FormAttachment( middle, -margin );
    fdlConnection.top = new FormAttachment( lastControl, 2 * margin );
    wlConnection.setLayoutData( fdlConnection );

    Button wEditConnection = new Button( wComposite, SWT.PUSH | SWT.BORDER );
    wEditConnection.setText( BaseMessages.getString( PKG, "System.Button.Edit" ) );
    FormData fdEditConnection = new FormData();
    fdEditConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdEditConnection.right = new FormAttachment( 100, 0 );
    wEditConnection.setLayoutData( fdEditConnection );

    Button wNewConnection = new Button( wComposite, SWT.PUSH | SWT.BORDER );
    wNewConnection.setText( BaseMessages.getString( PKG, "System.Button.New" ) );
    FormData fdNewConnection = new FormData();
    fdNewConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdNewConnection.right = new FormAttachment( wEditConnection, -margin );
    wNewConnection.setLayoutData( fdNewConnection );

    wConnection = new CCombo( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnection );
    wConnection.addModifyListener( lsMod );
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment( middle, 0 );
    fdConnection.right = new FormAttachment( wNewConnection, -margin );
    fdConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    wConnection.setLayoutData( fdConnection );
    lastControl = wConnection;
	
	 // collection line
    //Label
    Label wlcollection = new Label( wComposite, SWT.RIGHT );
    wlcollection.setText( "Collection" );//collection
    props.setLook( wlcollection );
    FormData fdlcollection = new FormData();
    fdlcollection.left = new FormAttachment( 0, 0 );
    fdlcollection.right = new FormAttachment( middle, -margin );
    fdlcollection.top = new FormAttachment( lastControl, margin );
    wlcollection.setLayoutData( fdlcollection ); 
	//Collectionbutton
    Button getCollectionButton = new Button(wComposite, SWT.PUSH | SWT.CENTER);
	getCollectionButton.setText("Get collections");
	props.setLook(getCollectionButton);
	FormData getCollectionButtonData = new FormData();
	getCollectionButtonData.top = new FormAttachment(lastControl, margin);
	getCollectionButtonData.right = new FormAttachment(100, 0);
	getCollectionButton.setLayoutData(getCollectionButtonData);
	//textfield
    wCollection = new CCombo( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCollection );
    wCollection.addModifyListener( lsMod );
    FormData fdcollection = new FormData();
    fdcollection.left = new FormAttachment( middle, 0 );
    fdcollection.top = new FormAttachment( lastControl, margin );
    fdcollection.right = new FormAttachment( getCollectionButton, 0 );
    wCollection.setLayoutData( fdcollection );
    lastControl = wCollection;
	
	
	
	//Insert/upsert type
	Label wlInsertType = new Label( wComposite, SWT.RIGHT );
    wlInsertType.setText( "Insert type" );
    props.setLook( wlInsertType );
    FormData fdlInsertType = new FormData();
    fdlInsertType.left = new FormAttachment( 0, 0 );
    fdlInsertType.right = new FormAttachment( middle, -margin );
    fdlInsertType.top = new FormAttachment( lastControl, 2 * margin );
    wlInsertType.setLayoutData( fdlInsertType );
	wInsertType = new CCombo( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInsertType );
    wInsertType.addModifyListener( lsMod );
    FormData fdInsertType = new FormData();
    fdInsertType.left = new FormAttachment( middle, 0 );
    fdInsertType.right = new FormAttachment(100, 0 );
    fdInsertType.top = new FormAttachment( wlInsertType, 0, SWT.CENTER );
    wInsertType.setLayoutData( fdInsertType );
    lastControl = wInsertType;
	
	//Key
	Label wlKey = new Label( wComposite, SWT.RIGHT );
    wlKey.setText( "Key" );
    props.setLook( wlKey );
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment( 0, 0 );
    fdlKey.right = new FormAttachment( middle, -margin );
    fdlKey.top = new FormAttachment( lastControl, 2 * margin );
    wlKey.setLayoutData( fdlKey );
	wKey = new CCombo( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wKey );
    wKey.addModifyListener( lsMod );
    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment( middle, 0 );
    fdKey.right = new FormAttachment(100, 0 );
    fdKey.top = new FormAttachment( wlKey, 0, SWT.CENTER );
    wKey.setLayoutData( fdKey );
    lastControl = wKey;
	
	//Value
	
	Label wlValue = new Label( wComposite, SWT.RIGHT );
    wlValue.setText( "Value" );
    props.setLook( wlValue );
    FormData fdlValue = new FormData();
    fdlValue.left = new FormAttachment( 0, 0 );
    fdlValue.right = new FormAttachment( middle, -margin );
    fdlValue.top = new FormAttachment( lastControl, 2 * margin );
    wlValue.setLayoutData( fdlValue );
	wValue = new CCombo( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wValue );
    wValue.addModifyListener( lsMod );
    FormData fdValue = new FormData();
    fdValue.left = new FormAttachment( middle, 0 );
    fdValue.right = new FormAttachment(100, 0 );
    fdValue.top = new FormAttachment( wlValue, 0, SWT.CENTER );
     wValue.setLayoutData( fdValue );
    lastControl = wValue;
	
	//Build JSON from fields
	/*Label wlValueFields = new Label( wComposite, SWT.LEFT );
    wlValueFields.setText( "or Build JSON Value from fields" );
    props.setLook( wlValueFields );
    FormData fdlValueFields = new FormData();
    fdlValueFields.left = new FormAttachment( 0, 0 );
    fdlValueFields.right = new FormAttachment( middle, -margin );
    fdlValueFields.top = new FormAttachment( lastControl, margin );
	wlValueFields.setLayoutData( fdlValueFields );
	 ColumnInfo[] valueFieldsColumns =
      new ColumnInfo[] {
        new ColumnInfo( "Field name", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Couchbase name", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Type", ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getAllValueMetaNames(), false ),
    };
	
	wValueFields = new TableView( transMeta, wComposite, SWT.FULL_SELECTION | SWT.MULTI, returnColumns, input.getReturnValues().size(), lsMod, props );
    props.setLook( wValueFields );
    wValueFields.addModifyListener( lsMod );
    FormData fdValueFields = new FormData();
    fdValueFields.left = new FormAttachment( 0, 0 );
    fdValueFields.right = new FormAttachment( wbGetReturnFields, 0 );
    fdValueFields.top = new FormAttachment( wlValueFields, margin );
    fdValueFields.bottom = new FormAttachment( wlValueFields, 300 + margin );
    wValueFields.setLayoutData( fdValueFields );
    lastControl = wValueFields;*/
	
	
    wComposite.pack();
    Rectangle bounds = wComposite.getBounds();

    wScrolledComposite.setContent( wComposite );
    wScrolledComposite.setExpandHorizontal( true );
    wScrolledComposite.setExpandVertical( true );
    wScrolledComposite.setMinWidth( bounds.width );
    wScrolledComposite.setMinHeight( bounds.height );


    wOK = new Button( wComposite, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( wComposite, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    // Position the buttons at the bottom of the dialog.
    //
    setButtonPositions( new Button[] { wOK,wCancel }, margin, null );

	
	// Add listeners
    //
    wCancel.addListener( SWT.Selection, e -> cancel() );
    wOK.addListener( SWT.Selection, e -> ok() );

    wConnection.addModifyListener( lsMod );
	wCollection.addModifyListener( lsMod );
	wInsertType.addModifyListener( lsMod );
	wKey.addModifyListener( lsMod );
	wValue.addModifyListener( lsMod );
    wStepname.addModifyListener( lsMod );
	
		
	// Chose collection Button
	/*collectionButton.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {    				
   				try {
						Couchbase couchbase;
						MetaStoreFactory<CouchbaseConnection> factory = CouchbaseConnectionUtil.getConnectionFactory(metaStore);
						CouchbaseConnection couchbaseConnection = factory.loadElement(wConnection.getText());
						couchbaseConnection.initializeVariablesFrom(transMeta);					
						String query="SHOW collectionS";						
						couchbase=couchbaseConnection.connectToCouchbase();
						
						QueryResult result = couchbase.query(new Query(query));
						if(result!=null)
						{
							List<List<Object>> collectionNames = result.getResults().get(0).getSeries().get(0).getValues();
									
							//List<String> collections = Lists.newArrayList();
							int selectedcollection= -1;
							int i=0;
							if (collectionNames != null) 
							{
								String[] collectionsList=new String[collectionNames.size()];

								for (List<Object> collection : collectionNames) {
									collectionsList[i]=(collection.get(0).toString());
									if(wCollection!=null && !wCollection.getText().isEmpty() && collectionsList[i].equals(wCollection.getText())){
										selectedcollection = i;	
									}
									i++;	
								}
								
								EnterSelectionDialog esd = new EnterSelectionDialog(shell, collectionsList, "collections", "Select a collection.");
								if (selectedcollection > -1) {
									esd.setSelectedNrs(new int[]{selectedcollection});
								}
								String s=esd.open();
								if(s!=null)
								{
									if (esd.getSelectionIndeces().length > 0) {
										selectedcollection = esd.getSelectionIndeces()[0];
										String db = collectionsList[selectedcollection];
										if(db!=null){
											wCollection.setText(db);
										}										
									} 
									else {
										wCollection.setText("");
									}
								}
								
							}
						}
						couchbase.close();
					} catch(Exception ex) {
					        new ErrorDialog( shell, "Error", "Error retrieving collections",ex );
					}
			}
	});*/
    wNewConnection.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        newConnection();
      }
    } );
    wEditConnection.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        editConnection();
      }
    } );
	getCollectionButton.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        setConnectionValues();
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;

  }


  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }
  
  private RowMetaInterface getPreviousFields() {
    RowMetaInterface previousFields;
    try {
      previousFields = transMeta.getPrevStepFields( stepMeta );
    } catch ( KettleStepException e ) {
      new ErrorDialog( shell, BaseMessages
          .getString( org.pentaho.di.couchbase.trans.steps.pentahocouchbase.PentahoCouchbasePluginOutputMeta.PKG,
              "System.Dialog.Error.Title" ), BaseMessages
          .getString( org.pentaho.di.couchbase.trans.steps.pentahocouchbase.PentahoCouchbasePluginOutputMeta.PKG,
              "CouchbaseDialog.ErrorDialog.UnableToGetInputFields.Message" ), e );
      previousFields = new RowMeta();
    }

    return previousFields;
  }
  
  public List<String> getCollections()
  {
	 List<String> collectionsNames = new ArrayList<String>(); 
	 //PentahoCouchbasePluginOutputMeta oneMeta = new PentahoCouchbasePluginOutputMeta();
     //this.getInfo( oneMeta );
	try{
   	 MetaStoreFactory<CouchbaseConnection> factory = CouchbaseConnectionUtil.getConnectionFactory( metaStore );
     CouchbaseConnection couchbaseConnection = factory.loadElement( wConnection.getText() );
     couchbaseConnection.initializeVariablesFrom( transMeta );
	 Bucket bucket=couchbaseConnection.connectToCouchbaseBucket();
	
	if(bucket==null)
	 {
		 logBasic("Impossible to get collections, bucket null");
		 return null;
	 }
	 
	 CollectionManager collectionManager = bucket.collections();
	
	  for (ScopeSpec scope : collectionManager.getAllScopes()) {
		//System.out.println("  Scope: " + scope.name());
        logBasic("found scope,"+scope.name());
		for (CollectionSpec collection : scope.collections()) {
		  logBasic("found collection,"+collection.name());
		  collectionsNames.add(scope.name() + collection.name());
		}
	  }
	 } catch (Exception ex) {
	     new ErrorDialog( shell, BaseMessages
          .getString( org.pentaho.di.couchbase.trans.steps.pentahocouchbase.PentahoCouchbasePluginOutputMeta.PKG,
              "System.Dialog.Error.Title" ), BaseMessages
          .getString( org.pentaho.di.couchbase.trans.steps.pentahocouchbase.PentahoCouchbasePluginOutputMeta.PKG,
              "CouchbaseDialog.ErrorDialog.getcollections.Message" ), ex );
			  return null;
	 }
	 return collectionsNames;
	 
	/* data.metaStore = MetaStoreUtil.findMetaStore( this );
	 couchbaseConnection = CouchbaseConnectionUtil.getConnectionFactory(data.metaStore).loadElement(environmentSubstitute(meta.getConnectionName()));
	 couchbaseConnection.initializeVariablesFrom(this);	*/
	 
  }


  public void getData() {

    wStepname.setText( Const.NVL( stepname, "" ) );
    wConnection.setText( Const.NVL( input.getConnectionName(), "" ) );
    wCollection.setText(Const.NVL(input.getCollection(),"default"));
	wInsertType.setText(Const.NVL(input.getInsertType(),"Upsert"));
	wValue.setText(Const.NVL(input.getValue(),""));
	wKey.setText(Const.NVL(input.getKey(),""));
   
    // List of connections...
    //
    try {
      List<String> elementNames = CouchbaseConnectionUtil.getConnectionFactory( metaStore ).getElementNames();
      Collections.sort( elementNames );
      wConnection.setItems( elementNames.toArray( new String[ 0 ] ) );
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Unable to list Couchbase connections", e );
    }
   

	//List of Inseret types...
	 List<String> elementNames = new ArrayList<String>();
     elementNames.add("Upsert");
	 elementNames.add("Insert");
	 elementNames.add("Batch - NA");
     wInsertType.setItems( elementNames.toArray( new String[ 0 ] ) );

    List<String> keyNames = new ArrayList<String>();
	List<String> valueNames = new ArrayList<String>();
	if ( !gotPreviousFields ) {
      try {
        String key = wKey.getText();
        String value = wValue.getText();
        RowMetaInterface r = getPreviousFields(  );
        if ( r != null ) {
          wKey.setItems( r.getFieldNames() );
          wValue.setItems( r.getFieldNames() );
        }
      
      } catch ( Exception ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "TableOutputDialog.FailedToGetFields.DialogTitle" ), BaseMessages
            .getString( PKG, "TableOutputDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }
   /* for ( int i = 0; i < input.getReturnValues().size(); i++ ) {
      keyNames.add(returnValue.getName());
	  valueNames.add(returnValue.getName());
	  /*ReturnValue returnValue = input.getReturnValues().get( i );
      TableItem item = wReturns.table.getItem( i );
      
	  item.setText( 1, Const.NVL( returnValue.getName(), "" ) );
      item.setText( 2, Const.NVL( returnValue.getCouchbaseName(), "" ) );
      item.setText( 3, Const.NVL( returnValue.getType(), "" ) );
      item.setText( 4, returnValue.getLength() < 0 ? "" : Integer.toString( returnValue.getLength() ) );
      item.setText( 5, Const.NVL( returnValue.getFormat(), "" ) );
    }*/
	//wKey.setItems( keyNames.toArray( new String[ 0 ] ) );
	//wValue.setItems( valueNames.toArray( new String[ 0 ] ) );
    //wReturns.removeEmptyRows();
   // wReturns.setRowNums();
    //wReturns.optWidth( true );

  

  private void ok() {
    if ( StringUtils.isEmpty( wStepname.getText() ) ) {
      return;
    }
    stepname = wStepname.getText(); // return value
    getInfo( input );
    dispose();
  }
  
  private void setConnectionValues(){
	  	try {
		List<String> collectionNames=getCollections();
		if(collectionNames!=null){
		wCollection.setItems( collectionNames.toArray( new String[ 0 ] ) );
		}
		else throw new KettleException( "No collections");
	
	   } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Unable to list Couchbase collections", e );
    }
  }
  
  private void getInfo( PentahoCouchbasePluginOutputMeta meta ) {
    meta.setConnectionName( wConnection.getText() );
    //meta.setQuery( wQuery.getText() );
	meta.setCollection(wCollection.getText());
	meta.setInsertType(wInsertType.getText());
	meta.setKey(wKey.getText());
	meta.setValue(wValue.getText());
	
	//meta.setVariables(wVariables.getSelection());

    /*List<ReturnValue> returnValues = new ArrayList<>();
    for ( int i = 0; i < wReturns.nrNonEmpty(); i++ ) {
      TableItem item = wReturns.getNonEmpty( i );
      String name = item.getText( 1 );
      String couchbaseName = item.getText( 2 );
      /*String type = item.getText( 3 );
      int length = Const.toInt( item.getText( 4 ), -1 );
      String format = item.getText( 5 );
      returnValues.add( new ReturnValue( name, couchbaseName) );
    }
    meta.setReturnValues( returnValues );*/
  }

  protected void newConnection() {
    CouchbaseConnection connection = CouchbaseConnectionUtil.newConnection( shell, transMeta, CouchbaseConnectionUtil.getConnectionFactory( metaStore ) );
    if ( connection != null ) {
      wConnection.setText( connection.getName() );
    }
  }

  protected void editConnection() {
    CouchbaseConnectionUtil.editConnection( shell, transMeta, CouchbaseConnectionUtil.getConnectionFactory( metaStore ), wConnection.getText() );
  }

  private synchronized void preview() {
    PentahoCouchbasePluginOutputMeta oneMeta = new PentahoCouchbasePluginOutputMeta();
    this.getInfo( oneMeta );
    TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation( this.transMeta, oneMeta, this.wStepname.getText() );
    this.transMeta.getVariable( "Internal.Transformation.Filename.Directory" );
    previewMeta.getVariable( "Internal.Transformation.Filename.Directory" );
    EnterNumberDialog
      numberDialog = new EnterNumberDialog( this.shell, this.props.getDefaultPreviewSize(),
      BaseMessages.getString( PKG, "QueryDialog.PreviewSize.DialogTitle" ),
      BaseMessages.getString( PKG, "QueryDialog.PreviewSize.DialogMessage" )
    );
    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      TransPreviewProgressDialog progressDialog = new TransPreviewProgressDialog( this.shell, previewMeta, new String[] { this.wStepname.getText() }, new int[] { previewSize } );
      progressDialog.open();
      Trans trans = progressDialog.getTrans();
      String loggingText = progressDialog.getLoggingText();
      if ( !progressDialog.isCancelled() && trans.getResult() != null && trans.getResult().getNrErrors() > 0L ) {
        EnterTextDialog etd = new EnterTextDialog( this.shell,
          BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title", new String[ 0 ] ),
          BaseMessages.getString( PKG, "System.Dialog.PreviewError.Message", new String[ 0 ] ), loggingText, true );
        etd.setReadOnly();
        etd.open();
      }

      PreviewRowsDialog prd = new PreviewRowsDialog( this.shell, this.transMeta, 0, this.wStepname.getText(), progressDialog.getPreviewRowsMeta( this.wStepname.getText() ),
        progressDialog.getPreviewRows( this.wStepname.getText() ), loggingText );
      prd.open();
    }
  }
  
  private String converType(String initType){
	  
      String destType="String";
	  if(initType==null || initType.isEmpty())
		  return destType;
	  switch(initType.toLowerCase())
	  {
		case "float" : destType ="Number"; break;
		case "boolean" : destType ="Boolean"; break;
		case "integer" : destType= "Integer"; break;
        case "string" : destType = "String"; break;
	  }	
      return destType;	  
  }

  
    /*private void getReturnValues() throws KettleException {

    try {

		  MetaStoreFactory<CouchbaseConnection> factory = CouchbaseConnectionUtil.getConnectionFactory( metaStore );
		  CouchbaseConnection couchbaseConnection = factory.loadElement(this.transMeta.environmentSubstitute(wConnection.getText()));
		  couchbaseConnection.initializeVariablesFrom( this.transMeta );
		  Couchbase couchbase;  
		  String query=this.transMeta.environmentSubstitute(wQuery.getText());
          		  
		  String collection=this.transMeta.environmentSubstitute(wCollection.getText());
		  couchbase=couchbaseConnection.connectToCouchbase();
		  //couchbase.setcollection(collection);
		  
		  //Working on query to extract tags, columns
		  if(query!=null && !query.isEmpty())
		  {
			  TableItem itemTime = new TableItem(wReturns.table, SWT.NONE);
              itemTime.setText(1, "time");
			  itemTime.setText(2, "Time");
			  itemTime.setText(3, "Timestamp");
			  itemTime.setText(4, "");
			  itemTime.setText(5, "yyyy-MM-dd'T'HH:mm:ss'Z'");			  
			 
			  int startPos=query.toLowerCase().indexOf("from");
			  String showTagsQuery="SHOW TAG KEYS ON "+collection+" "+query.substring(startPos);
			  
			  QueryResult result = couchbase.query(new Query(showTagsQuery));
			  List<List<Object>> columns = result.getResults().get(0).getSeries().get(0).getValues();

			  if (columns != null) 
				{
					for (List<Object> column : columns) {
							if(column!=null && column.get(0)!=null && !column.get(0).toString().isEmpty())
							{
								TableItem item = new TableItem(wReturns.table, SWT.NONE);
								item.setText(1, column.get(0).toString());
								item.setText(2, "Tag");
								item.setText(3, "String");
								item.setText(4, "");
								item.setText(5, "");
							}
						}
						
				}
			 String showFieldsQuery="SHOW FIELD KEYS ON "+collection+" "+query.substring(startPos);
			 result = couchbase.query(new Query(showFieldsQuery));
			 List<List<Object>> values = result.getResults().get(0).getSeries().get(0).getValues();
			 
			 if (values != null) 
				{
					for (List<Object> fields : values) {
						if(columns!=null && !columns.isEmpty()){
							TableItem itemF = new TableItem(wReturns.table, SWT.NONE);
							itemF.setText(1, fields.get(0).toString());
							itemF.setText(2, "Field");
							itemF.setText(3, converType(fields.get(1).toString()));
							itemF.setText(4, "");
							itemF.setText(5, "");
							}
						}
						
				}
		  }
		  /*Map<String, Object> tags = new HashMap<String, Object>(); 
		  if(result.getResults().get(0).getSeries().get(0).getTags()!=null) {
			tags.putAll(result.getResults().get(0).getSeries().get(0).getTags());
          }

		  if (tags != null) 
			{
				for (Map.Entry<String, Object> entry : tags.entrySet()) {
					//System.out.println("Item : " + entry.getKey() + " Count : " + entry.getValue());
					TableItem item = new TableItem(wReturns.table, SWT.NONE);
					item.setText(1, entry.getKey());
					item.setText(2, "Tag");
					item.setText(3, "String");
				}											
			}
		 
		 couchbase.close();
		
		
		} catch(Exception e) {
		  throw new KettleException( "Error connecting to Couchbase connection to get collections", e );
		}
	  

    
        
     }*/
	 



}

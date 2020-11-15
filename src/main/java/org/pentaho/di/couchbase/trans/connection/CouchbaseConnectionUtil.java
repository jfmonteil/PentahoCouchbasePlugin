package org.pentaho.di.couchbase.trans.connection;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.util.PentahoDefaults;
import java.io.IOException;
import org.pentaho.di.couchbase.trans.metastore.MetaStoreFactory;
import org.pentaho.di.couchbase.trans.connection.CouchbaseConnectionDialog;

public class CouchbaseConnectionUtil {
  private static Class<?> PKG = CouchbaseConnectionUtil.class; // for i18n purposes, needed by Translator2!!

  public static MetaStoreFactory<CouchbaseConnection> getConnectionFactory( IMetaStore metaStore ) {
    return new MetaStoreFactory<CouchbaseConnection>( CouchbaseConnection.class, metaStore, PentahoDefaults.NAMESPACE );
  }

  public static CouchbaseConnection newConnection( Shell shell, VariableSpace space, MetaStoreFactory<CouchbaseConnection> factory ) {

    CouchbaseConnection connection = new CouchbaseConnection( space );
    boolean ok = false;
    while ( !ok ) {
      CouchbaseConnectionDialog dialog = new CouchbaseConnectionDialog( shell, connection );
      if ( dialog.open() ) {
        // write to metastore...
        try {
          if ( factory.loadElement( connection.getName() ) != null ) {
            MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_ERROR );
            box.setText( BaseMessages.getString( PKG, "CouchbaseConnectionUtil.Error.ConnectionExists.Title" ) );
            box.setMessage( BaseMessages.getString( PKG, "CouchbaseConnectionUtil.Error.ConnectionExists.Message" ) );
            int answer = box.open();
            if ( ( answer & SWT.YES ) != 0 ) {
              factory.saveElement( connection );
              ok = true;
            }
          } else {
            factory.saveElement( connection );
            ok = true;
          }
        } catch ( Exception exception ) {
          new ErrorDialog( shell,
            BaseMessages.getString( PKG, "CouchbaseConnectionUtil.Error.ErrorSavingConnection.Title" ),
            BaseMessages.getString( PKG, "CouchbaseConnectionUtil.Error.ErrorSavingConnection.Message" ),
            exception );
          return null;
        }
      } else {
        // Cancel
        return null;
      }
    }
    return connection;
  }

  public static void editConnection( Shell shell, VariableSpace space, MetaStoreFactory<CouchbaseConnection> factory, String connectionName ) {
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }
    try {
      CouchbaseConnection CouchbaseConnection = factory.loadElement( connectionName );
      CouchbaseConnection.initializeVariablesFrom( space );
      if ( CouchbaseConnection == null ) {
        newConnection( shell, space, factory );
      } else {
        CouchbaseConnectionDialog CouchbaseConnectionDialog = new CouchbaseConnectionDialog( shell, CouchbaseConnection );
        if ( CouchbaseConnectionDialog.open() ) {
          factory.saveElement( CouchbaseConnection );
        }
      }
    } catch ( Exception exception ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "CouchbaseConnectionUtil.Error.ErrorEditingConnection.Title" ),
        BaseMessages.getString( PKG, "CouchbaseConnectionUtil.Error.ErrorEditingConnection.Message" ),
        exception );
    }
  }

  public static void deleteConnection( Shell shell, MetaStoreFactory<CouchbaseConnection> factory, String connectionName ) {
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }

    MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_ERROR );
    box.setText( BaseMessages.getString( PKG, "CouchbaseConnectionUtil.DeleteConnectionConfirmation.Title" ) );
    box.setMessage( BaseMessages.getString( PKG, "CouchbaseConnectionUtil.DeleteConnectionConfirmation.Message", connectionName ) );
    int answer = box.open();
    if ( ( answer & SWT.YES ) != 0 ) {
      try {
        factory.deleteElement( connectionName );
      } catch ( Exception exception ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "CouchbaseConnectionUtil.Error.ErrorDeletingConnection.Title" ),
          BaseMessages.getString( PKG, "CouchbaseConnectionUtil.Error.ErrorDeletingConnection.Message", connectionName ),
          exception );
      }
    }
  }

}

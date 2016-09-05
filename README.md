# dbPDO
My own PDO class

I like the php PDO extension: your program can use any database supported by this extension

PDO_DBLIB   FreeTDS / Microsoft SQL Server / Sybase
PDO_MYSQL   MySQL 3.x/4.x/5.x
PDO_PGSQL   PostgreSQL
PDO_SQLITE  SQLite 3 and SQLite 2
PDO_SQLSRV  Microsoft SQL Server / SQL Azure

The constructor allows the setting of some additional parameters so that the extension may be configured to one's needs.

@param string $file  the name of a file containing the parameters or an array containing the parameters.

These are as follows:
	 [
	     'db_driver' => 'mysql',                       // The database driver
	     'db_user' => 'username',                      // The user
	     'db_password' => 'password',                  // The password
	     'db_blowfish' => '************************',    // An string used to encrypt data
	     'dsn' => [
	         'host' => 'localhost',                    // The database server
	         'port' => 3306,                           // The database port
	         'dbname' => 'example_database',           // The database name
	     ],
	     'db_options' => [
	         'PDO::ATTR_PERSISTENT' => 1,
	     ],                                            // The options you want to use
	     'db_attributes' => [
	         'ATTR_ERRMODE' => 'ERRMODE_EXCEPTION',
	     ],  // The attributes you want to set
	     'log_sql' => true,                            // True to log the sql
	     'log_filename' => 'log.sql',                  // SQL log filename
	     'last_sql' => true,                           // True to get last prepared SQL
	     'last_sql_value' => 'use example_database',   // Default value for last prepared SQL
	 ];

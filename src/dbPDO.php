<?php
/**
 *
 * dbPDO - my own PDO class
 *
 * @category   Libraries
 * @package    Database Utilities
 * @subpackage dbPDO
 * @license    http://opensource.org/licenses/mit-license.html MIT License
 * @author     Gustavo Adolfo D'Anetra <webmaster@gdanetra.net>
 * @copyright  2013-2016 Gustavo Adolfo D'Anetra
 * @link       http://www.gdanetra.net
 * @version    1.5.5
 *
 * I like the php PDO extension: your program can use any database supported by this extension
 *
 * PDO_DBLIB   FreeTDS / Microsoft SQL Server / Sybase
 * PDO_MYSQL   MySQL 3.x/4.x/5.x
 * PDO_PGSQL   PostgreSQL
 * PDO_SQLITE  SQLite 3 and SQLite 2
 * PDO_SQLSRV  Microsoft SQL Server / SQL Azure
 *
 * I have used dblib to access Microsoft SQL Server databases on Apache
 *
 * 1.4 - If a column value is an array encode it in json notation.
 * 1.5 - Add private methods quoteTableName and fieldAlias.
 * 1.5.1 - Adjust CRUD methods
 * 1.5.2 - Correct BUG (connection)
 * 1.5.3 - Constant Revision
 * 1.5.4 - select_record Updated
 * 1.5.5 - password_hash
 *
 */

class dbPDO extends \PDO
{
	/**
 	 * @access private
	 * @var array
	 */
	var $_config = array();
	/**
	 * @var array
	 */
	var $implemented = array(
		'PDO_DBLIB' => true,      // FreeTDS / Microsoft SQL Server / Sybase
		'PDO_MYSQL'  => true,     // MySQL 3.x/4.x/5.x
		'PDO_PGSQL' => true,      // PostgreSQL
		'PDO_SQLITE' => true,     // SQLite 3 and SQLite 2
		'PDO_SQLSRV' => false     // Microsoft SQL Server / SQL Azure
	);
	/**
 	 * @access protected
	 * @var bool
	 */
	protected $hasActiveTransaction = false;
	/**
	 * @access protected
	 * @var bool
	 */
	protected $schema = false;
	/**
	 * @access protected
	 * @var bool
	 */
	protected $catalog = false;
	/* Last Query Info */
	/**
	 * @var string
	 */
	var $sql = '';
	/**
	 * @var string
	 */
	var $get_sql = '';
	/**
	 * @var string
	 */
	var $get_page = '';
	/**
	 * @var string
	 */
	var $get_limit = '';
	/**
	 * @var string
	 */
	var $get_total_pages = '';
	/**
	 * @var string
	 */
	var $get_total_rows = '';
	/**
	 * @var string
	 */
	var $stmt = '';
	/**
	 * @var array
	 */
	var $current = array();

	/**
	 * Exception Handler
	 *
	 * Personalize for your convenience
	 *
	 */
	public static function exception_handler($exception)
	{
		// Output the exception details
		die(LANG_UNCAUGHT_EXCEPTION.$exception->getMessage());
	}

	/**
	 * Constructor
	 *
	 * The constructor allows the setting of some additional
	 * parameters so that the extension may be configured to
	 * one's needs.
	 *
	 * @param string $file  the name of a file containing the parameters
	 *                      or an array containing the parameters.
	 *
	 * These are as follows:
	 * array (
	 *    'db_driver' => 'mysql',                       // The database driver
	 *    'db_user' => 'username',                      // The user
	 *    'db_password' => 'password',                  // The password
	 *    'db_blowfish' => '4Af7M6cP28xwDrt5AKsMWB',    // An string used to encrypt data
	 *    'dsn' => array (
	 *        'host' => 'localhost',                    // The database server
	 *        'port' => 3306,                           // The database port
	 *        'dbname' => 'example_database' ),         // The database name
	 *    'db_options' => array (
	 *        'PDO::ATTR_PERSISTENT' => 1 ),            // The options you want to use
	 *    'db_attributes' => array (
	 *        'ATTR_ERRMODE' => 'ERRMODE_EXCEPTION' ),  // The attributes you want to set
	 *    'log_sql' => true,                            // True to log the sql
	 *    'log_filename' => 'log.sql',                  // SQL log filename
	 *    'last_sql' => true,                           // True to get last prepared SQL
	 *    'last_sql_value' => 'use example_database'    // Default value for last prepared SQL
	 * );
	 *
	 */
	public function __construct($file='dbConfig.ini')
	{
		if (is_array($file)) {
				$this->_config = $file;
		} else {
				if (!$this->_config = parse_ini_file($file, TRUE)) throw new exception('Unable to open ' . $file . '.');
		}
		$dbDriver = $this->_config['db_driver'] ;
		$dsn = $dbDriver.':' ;
		reset($this->_config['dsn']);
		while (list($k, $v) = each ($this->_config['dsn'])) {
			$dsn .= "${k}=${v};" ;
		}
		// Temporarily change the PHP exception handler while we . . .
		set_exception_handler(array(__CLASS__, 'exception_handler'));
		parent::__construct($dsn, $this->_config['db_user'], $this->_config['db_password'], $this->_config['db_options']);
		reset($this->_config['db_attributes']);
		while (list($k, $v) = each ($this->_config['db_attributes'])) {
			parent::setAttribute ( constant ( "\PDO::{$k}" ), constant ( "\PDO::{$v}" ) ) ;
		}
		// Set table schema
		$this->schema = $this->_config['dsn']['dbname'];
		// Set table catalog for pgsql, mssql and dblib drivers
		switch ($dbDriver) {
		case 'dblib':
		case 'pgsql':
		case 'mssql':
		case 'sqlsrv':
			$stmt = parent::query("SELECT table_catalog as catalog FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '{$this->schema}';");
			$row = $stmt->fetch(PDO::FETCH_ASSOC);
			$this->catalog = $row['catalog'];
			break;
		default:
			break;
		}
		// Change the exception handler back to whatever it was before
		restore_exception_handler();
	}

	/**
	 * To avoid copies
	 *
	 * @access protected
	 */
	protected function __clone(){}

	/**
	 * To print the database name
	 */
	public function __toString()
	{
		return $this->dsn['dbname'];
	}

	/**
	 * To encapsulate tabnames
	 *
	 * @access protected
	 */
	protected function quoteTableName($table, $quote=null)
	{
		$driver = $this->getDriverName();
		if (null === $quote) {
			switch ($driver) {
			case 'mysql':
				$quotes = array('`','`');
				break;
			case 'pgsql':
				$quotes = array('','');
				break;
			case 'dblib':
				$quotes = array('[',']');
				break;
			case 'mssql':
			case 'sqlsrv':
				$quotes = array('"','"');
				break;
			default:
				$quotes = array('','');
				break;
			}
		} else {
			$quotes = $quote;
		}
		if (strpos($table, '.') > 0) {
			$table = preg_replace('/\./', $quotes[1].'.'.$quotes[0], $table);
		}
		return $quotes[0].$table.$quotes[1];
	}

	/**
	 * Create field alias names
	 *
	 * @access protected
	 * @param string $field Create a field name alias
	 * @return string
	 */
	protected function fieldAlias($field)
	{
		if ($p = strpos($field, '.')) {
			$field = substr($field,$p+1);
		}
		return preg_replace('/ /', '_', $field);
	}

	/**
	 * To enable/disable the log file
	 */
	public function setLog($log_filename='log.sql')
	{
		$this->_config['log_sql'] = true;
		$this->_config['log_filename'] = $log_filename;
	}

	/**
	 *
	 */
	public function unsetLog()
	{
		$this->_config['log_sql'] = false;
	}

	/**
	 * To enable/disable the last sql option
	 */
	public function setLastSQL($last_sql_value='use')
	{
		$this->_config['last_sql'] = true;
		$this->_config['last_sql_value'] = $last_sql_value;
	}

	/**
	 *
	 */
	public function unsetLastSQL()
	{
		$this->_config['last_sql'] = false;
	}

	/**
	 * set quotes (',",`) for an unquoted string
	 *
	 * @param   string  $value          string to quote
	 * @param   string  $parameter_type type of value
	 * @return  string  quoted string
	 */
	public function quote($value, $parameter_type=PDO::PARAM_STR )
	{
		if( is_null($value) ) {
			return 'NULL';
		}
		return parent::quote($value, $parameter_type);
	}

	/**
	 * removes quotes (',",`) from a quoted string
	 *
	 * checks if the sting is quoted and removes this quotes
	 *
	 * @param   string  $quoted_string  string to remove quotes from
	 * @param   string  $quote          type of quote to remove
	 * @return  string  unquoted string
	 */
	function unQuote($quoted_string, $quote=null)
	{
		$quotes = array();
		if (null === $quote) {
			$quotes[] = '`';
			$quotes[] = '"';
			$quotes[] = "'";
		} else {
			$quotes[] = $quote;
		}
		foreach ($quotes as $quote) {
			if (substr($quoted_string, 0, 1) === $quote
			&& substr($quoted_string, -1, 1) === $quote) {
				$unquoted_string = substr($quoted_string, 1, -1);
				// replace escaped quotes
				$unquoted_string = str_replace($quote . $quote, $quote, $unquoted_string);
				return $unquoted_string;
			}
		}
		return $quoted_string;
  }

  /**
   * Construct an sql statement
   * Returns a SQL statement
   */
  function sql_clause($table, $cols, $fields, $where=false, $orderby=false, $limit=false)
  {
    if (!is_array($fields)) {
      return false;
    }
    $tabname = $this->quoteTableName($table);
    $driver = $this->getDriverName();
    if (!is_array($cols)) {
      $columns = '*';
    } else {
      //
      $sep = '';
      $columns = '';
      reset($cols);
      while (list($key, $val) = each($cols)) {
        $columns .= $sep;
        switch ($driver) {
          case 'mysql':
            $columns .= " `${key}`";
            break;
          case 'pgsql':
            $columns .= "${key}";
            break;
          case 'dblib':
            $columns .= "[${key}]";
            break;
          case 'mssql':
          case 'sqlsrv':
            $columns .= "[${key}]";
            break;
          default:
            $columns .= "${key}";
        }
        if ($val === '') {
          $val = $key;
        } elseif ($val === null) {
          $val = $key;
        }
        if ($key !== $val) {
          $columns .= ' as ' . preg_replace('/ /','_', $val);
        }
        $sep = ',';
      }

    }
    $sql = "SELECT ${columns} FROM ${tabname}";
    if ($where === null) {
      $sep = 'WHERE';
    } elseif ($where === false) {
      $sep = 'WHERE';
    } else {
      $sql .= ' WHERE '.$where;
      $sep = 'AND';
    }
    reset($fields);
    while (list($key, $val) = each($fields)) {
      switch ($driver) {
        case 'mysql':
          $field = "`${key}`";
          break;
        case 'pgsql':
          $field = "${key}";
          break;
        case 'dblib':
          //$field = "[${key}] as ".$this->fieldAlias($key);
          $field = "[${key}]";
          break;
        case 'mssql':
        case 'sqlsrv':
          $field = "[${key}]";
          break;
        default:
          $field = "${key}";
      }
      if ($pos = strpos($val,'%')) {
        $sql .= " {$sep} {$field} LIKE :${key}";
      } else {
        $sql .= " {$sep} {$field} = :${key}";
      }
      $sep = 'AND';
    }
    if ($limit) {
      switch ($driver) {
        case 'mysql':
          if ($orderby) {
            $sql .= ' ORDER BY '.$orderby;
          }
          $sql .= ' LIMIT '.$limit;
          break;
        case 'pgsql':
          if ($orderby) {
            $sql .= ' ORDER BY '.$orderby;
          }
          if ($pos = strpos(',',$limit)) {
            list($start,$count) = explode(',',$limit);
            $sql .= ' LIMIT '.$count.' OFFSET '.$start;
          } else {
            $sql .= ' LIMIT '.$limit;
          }
          break;
        case 'dblib':
        case 'mssql':
        case 'sqlsrv':
          if ($pos = strpos(',',$limit)) {
            if ($orderby) {
              if ($pos = strpos(',',$orderby)) {
                $orderFields = explode(',',$orderby);
                $rorderFields = array();
                reset($orderFields);
                while (list($k, $field) = each($orderFields)) {
                  if ($i = strpos(' asc',$orderby)) {
                    $rorderFields[] = preg_replace('/ asc/',' desc',$field);
                  } elseif ($j = strpos(' desc',$orderby)) {
                    $rorderFields[] = preg_replace('/ desc/',' asc',$field);
                  } elseif ($i = strpos(' ASC',$orderby)) {
                    $rorderFields[] = preg_replace('/ ASC/',' desc',$field);
                  } elseif ($j = strpos(' DESC',$orderby)) {
                    $rorderFields[] = preg_replace('/ DESC/',' asc',$field);
                  } else {
                    $orderFields[$k] = $field.' asc';
                    $rorderFields[] = $field.' desc';
                  }
                }
                $orderby = ' ORDER BY '.implode(',',$orderFields);
                $rorderby = ' ORDER BY '.implode(',',$rorderFields);
              } elseif ($i = strpos(' asc',$orderby)) {
                $orderby = ' ORDER BY '.$orderby;
                $rorderby = preg_replace('/ asc/',' desc',$orderby);
              } elseif ($j = strpos(' desc',$orderby)) {
                $orderby = ' ORDER BY '.$orderby;
                $rorderby = preg_replace('/ desc/',' asc',$orderby);
              } elseif ($i = strpos(' ASC',$orderby)) {
                $orderby = ' ORDER BY '.$orderby;
                $rorderby = preg_replace('/ ASC/',' desc',$orderby);
              } elseif ($j = strpos(' DESC',$orderby)) {
                $orderby = ' ORDER BY '.$orderby;
                $rorderby = preg_replace('/ DESC/',' asc',$orderby);
              } else {
                $rorderby = ' ORDER BY '.$orderby;
                $orderby = $rorderby.' asc';
                $rorderby = $rorderby.' desc';
              }
            } else {
              $orderby .= ' ORDER BY 1 asc';
              $rorderby .= ' ORDER BY 1 desc';
            }
            list($start,$count) = explode(',',$limit);
            $top = $start+$count;
            $total = $this->executeRecordCount($sql,$fields);
            if ($top > $total) {
              $top = $total;
              $count = $top - $start;
            }
            $sql = preg_replace('/SELECT /','SELECT * FROM (SELECT TOP '.$count.' * FROM (SELECT TOP '.$top.' ',$sql).$orderby.') AS NewTable1 '.$rorderby.') AS NewTable2 '.$orderby;
          } else {
            if ($orderby) {
              $sql = preg_replace('/SELECT /','SELECT TOP '.$limit.' ',$sql.' ORDER BY '.$orderby);
            } else {
              $sql = preg_replace('/SELECT /','SELECT TOP '.$limit.' ',$sql);
            }
          }
          break;
        default:
          if ($orderby) {
            $sql .= ' ORDER BY '.$orderby;
          }
          $sql .= ' LIMIT '.$limit;
          break;
      }
    } elseif ($orderby) {
      $sql .= ' ORDER BY '.$orderby;
    }
    return $sql;
  }

  /**
   * Select Table record(s)
   * Returns a SQL stament result
   */
  function select_record($table, $fields, $where=false, $orderby=false, $limit=false)
  {
    if (!is_array($fields)) {
      return false;
    }
    $this->sql = $this->sql_clause($table, '*', $fields, $where, $orderby, $limit);
		$this->stmt = $this->prepare($this->sql);
		$this->stmt->execute($fields);
		return $this->stmt->fetch(PDO::FETCH_ASSOC);
	}

	/**
	 * Select Table record(s)
	 * Returns a SQL stament result
	 */
	function get_records_page($table, $fields, $page, $limit=20, $sidx=1, $sord='asc')
	{
		$tabname = $this->quoteTableName($table);
		$sqlClause = "SELECT * FROM ${tabname}";
		$this->get_total_rows = $this->queryRecordCount($sqlClause);
		$this->get_limit = $limit;
		$this->get_page = $page;
		if( $this->get_total_rows > 0 ) {
			$this->get_total_pages = ceil($this->get_total_rows/$this->get_limit);
		} else {
			$this->get_total_pages = 0;
		}
		if ($this->get_page > $this->get_total_pages) {
			$this->get_page = $this->get_total_pages;
		}
		$start = ($this->get_limit * $this->get_page) - $this->get_limit;
		if ($start<0) {
			$start = 0;
		}
		return $this->select_record($tabname, $fields, false, "{$sidx} {$sord}", "{$start},{$limit}");
	}

	/**
	 * Returns the record count from a query
	 */
	public function queryRecordCount($sqlClause)
	{
		$sqlClause = trim($sqlClause);
		$sth = parent::query("SELECT count(*) as RecordCount FROM (${sqlClause}) as CntTbl");
		$row = $sth->fetch(PDO::FETCH_ASSOC);
		return $row['RecordCount'];
	}

	/**
	 * Returns the record count from a query
	 */
	public function executeRecordCount($sqlClause, $data=false)
	{
		if (!$data) {
			return $this->queryRecordCount($sqlClause);
		}
		$sqlClause = trim($sqlClause);
		$sth = $this->prepare("SELECT count(*) as RecordCount FROM (${sqlClause}) as CntTbl");
		$sth->execute($data);
		$row = $sth->fetch(PDO::FETCH_ASSOC);
		return $row['RecordCount'];
	}

	/**
	 * Returns the driver name from this DB handler
	 */
	public function getDriverName()
	{
		// return parent::getAttribute(PDO::ATTR_DRIVER_NAME);
		return $this->_config['db_driver'];
	}

	/**
	 * 1. Log the SQL clause
	 * 2. Save the SQL clause
	 * 3. Prepare the clause
	 */
	public function prepare($statement, $options=array())
	{
		if ($this->_config['log_sql']) {
			$logfile = fopen($this->_config['log_filename'], "a");
			fwrite($logfile, "${statement}\n");
			fclose($logfile);
		}
		if ($this->_config['last_sql']) {
			$this->_config['last_sql_value'] = $statement;
		}
		return parent::prepare($statement, $options);
	}

	/**
	 * Fetch a record from a statement
	 */
	public function fetchAssoc($stmt)
	{
			return $stmt->fetch(PDO::FETCH_ASSOC);
	}

	/**
	 * @param $stmt
	 * @return mixed
	 */
	public function fetchNum($stmt)
	{
		return $stmt->fetch(PDO::FETCH_NUM);
	}

  /**
	 *
	 * @access private
	 * @return string
	 */
	private function getBlowfishKey() {
		return '$2a$'.$this->_config['db_blowfish'];
	}

	/**
	 * @param string $v
	 * @return string
	 */
	public function getCrypt($v='') {
		return crypt($v, $this->getBlowfishKey());
	}

	/**
	 * AES encryption
	 */
	public function aesEncrypt($value)
	{
		$data = array();
		$data['value'] = $value;
		$data['salt'] = $this->_config['db_blowfish'];
		$sql = 'SELECT AES_ENCRYPT( :value, :salt ) as aesEncrypt';
		$stmt = parent::prepare($sql);
		$stmt->execute($data);
		$row = $stmt->fetch(PDO::FETCH_NUM);
		return base64_encode($row['aesEncrypt']);
	}

	/**
	 * @param $value
	 * @return mixed
	 */
	public function aesDecrypt($value)
	{
		$data = array();
		$data['value'] = base64_decode($value);
		$data['salt'] = $this->_config['db_blowfish'];
		$sql = 'SELECT AES_DECRYPT( :value, :salt ) as aesDecrypt';
		$stmt = parent::prepare($sql);
		$stmt->execute($data);
		$row = $stmt->fetch(PDO::FETCH_NUM);
		return $row['aesDecrypt'];
	}

	/**
	 * Returns the last prepared SQL clause
	 */
	public function last_prepared_sql()
	{
		if ($this->_config['last_sql']) {
			return false;
		}
		return $this->_config['last_sql_value'];
	}

	/**
	 *! @function beginTransaction
	 *  @abstract Checks if a transaction has been already active or begins a new
	 *            transaction
	 */
	function beginTransaction()
	{
		if ( !$this->inTransaction() ) {
			if ($this->hasActiveTransaction = parent::beginTransaction ()) {
				if ($this->_config['log_sql']) {
					$logfile = fopen($this->_config['log_filename'], "a");
					fwrite($logfile, ">>> BEGIN TRANSACTION\n");
					fclose($logfile);
				}
			}
		}
		return $this->hasActiveTransaction;
	}

	/**
	 *! @function commit
	 *  @abstract Checks if a transaction has been already active an commits it
	 */
	function commit()
	{
		if ( $this->inTransaction() ) {
			parent::commit ();
			if ($this->_config['log_sql']) {
				$logfile = fopen($this->_config['log_filename'], "a");
				fwrite($logfile, "<<< COMMIT TRANSACTION\n");
				fclose($logfile);
			}
			$this->hasActiveTransaction = false;
		}
	}

	/**
	 *! @function rollback
	 *  @abstract Checks if a transaction has been already active an commits it
	 */
	function rollback()
	{
		if ( $this->inTransaction() ) {
			parent::rollback ();
			if ($this->_config['log_sql']) {
				$logfile = fopen($this->_config['log_filename'], "a");
				fwrite($logfile, "<<< ROLLBACK TRANSACTION\n");
				fclose($logfile);
			}
			$this->hasActiveTransaction = false;
		}
	}

	/**
	 *! @function optimizeDataBase
	 *  @abstract Executed upon destruction of class instance to perform
	 *            repair, optimize and flush commands on each table in database
	 */
	public function optimizeDataBase()
	{
		switch ($this->getDriverName()) {
        case 'mysql':
			$dbTables = 'Tables_in_'.$this->dsn['dbname'];
			$obj = $this->query('SHOW TABLES');
			$results = $this->results($obj);
			foreach($results as $key => $value) {
				if (isset($value[$dbTables])) {
					$this->query('REPAIR TABLE '.$value[$dbTables]);
					$this->query('OPTIMIZE TABLE '.$value[$dbTables]);
					$this->query('FLUSH TABLE '.$value[$dbTables]);
				}
			}
			break;
        default:
			break;
		}
	}

	/**
	 *! @function insert
	 *  @abstract Creates and executes an insert sql statement with mysql syntax
	 *  @param string $tabname Table name
	 *  @param array  $data    Row data values, index keys are column names
	 *
	 *  TODO: extent this method to all database types syntax
	 */
	public function insert($tabname, $data)
	{
		$table = $this->quoteTableName($tabname);
		$fieldvalues = ':'.implode(', :', array_keys($data));
		switch ($this->getDriverName()) {
		case 'mysql':
			$fieldnames = '`'.implode('`, `', array_keys($data)).'`';
			$sql = "INSERT INTO ${table} ({$fieldnames}) VALUES ({$fieldvalues})";
			break;
		case 'pgsql':
			$fieldnames = implode(',', array_keys($data));
			$sql = "INSERT INTO ${table} ({$fieldnames}) VALUES ({$fieldvalues})";
			break;
		case 'dblib':
		case 'mssql':
		case 'sqlsrv':
			$fieldnames = '['.implode('], [', array_keys($data)).']';
			$sql = "INSERT INTO ${table} ({$fieldnames}) VALUES ({$fieldvalues})";
			break;
		default:
			$fieldnames = implode(',', array_keys($data));
			$sql = "INSERT INTO ${table} ({$fieldnames}) VALUES ({$fieldvalues})";
			break;
		}
		$outFields = array();
		foreach ($data as $key => $val) {
			if (is_array($val)) {
				$outFields[$key] = json_encode($val);
			} else {
				$outFields[$key] = $val;
			}
		}
		$sth = $this->prepare($sql);
		return $sth->execute($outFields);
	}

	/**
	 *! @function replace
	 *  @abstract Creates and executes an replace sql statement with mysql syntax
	 *  @param string $tabname Table name
	 *  @param array  $data    Row data values, index keys are column names
	 *
	 *  TODO: extent this method to all database types syntax
	 */
	public function replace($tabname, $data)
	{
		$table = $this->quoteTableName($tabname);
		$fieldvalues = ':'.implode(', :', array_keys($data));
		switch ($this->getDriverName()) {
		case 'mysql':
			$fieldnames = '`'.implode('`, `', array_keys($data)).'`';
			break;
		case 'pgsql':
			$fieldnames = implode(',', array_keys($data));
			break;
		case 'dblib':
		case 'mssql':
		case 'sqlsrv':
			$fieldnames = '['.implode('], [', array_keys($data)).']';
			break;
		default:
			$fieldnames = implode(',', array_keys($data));
			break;
		}
		$sql = "REPLACE INTO ${table} ({$fieldnames}) VALUES ({$fieldvalues})";
		$outFields = array();
		foreach ($data as $key => $val) {
			if (is_array($val)) {
				$outFields[$key] = json_encode($val);
			} else {
				$outFields[$key] = $val;
			}
		}
		$sth = $this->prepare($sql);
		return $sth->execute($outFields);
	}

	/**
	 *! @function update
	 *  @abstract Creates and executes an insert sql statement with mysql syntax
	 *  @param string $tabname Table name
	 *  @param array  $data    Row data values, index keys are column names
	 *  @param array  $keys    Key values, index keys are column names
	 *
	 *  TODO: extent this method to all database types syntax
	 */
	public function update($tabname, $data, $keys)
	{
		$table = $this->quoteTableName($tabname);
		$setFields = array();
		$outFields = array();
		foreach ($data as $key => $val) {
			switch ($this->getDriverName()) {
			case 'mysql':
				$setFields[] = "`${key}`=?";
				break;
			case 'pgsql':
				$setFields[] = "${key}=?";
				break;
			case 'dblib':
			case 'mssql':
			case 'sqlsrv':
				$setFields[] = "[${key}]=?";
				break;
			default:
				$setFields[] = "${key}=?";
			}
			if (is_array($val)) {
				$outFields[] = json_encode($val);
			} else {
				$outFields[] = $val;
			}
		}
		$whereKeys = array();
		foreach ($keys as $key => $val) {
			switch ($this->getDriverName()) {
			case 'mysql':
				$whereKeys[] = "`${key}`=?";
				break;
			case 'pgsql':
				$whereKeys[] = "${key}=?";
				break;
			case 'dblib':
			case 'mssql':
			case 'sqlsrv':
				$whereKeys[] = "[${key}]=?";
				break;
			default:
				$whereKeys[] = "${key}=?";
			}
			if (is_array($val)) {
					$outFields[] = json_encode($val);
			} else {
					$outFields[] = addslashes($val);
			}
		}
		$sql = "UPDATE ${table} SET " . implode(',', $setFields) . ' WHERE ' . implode(' AND ', $whereKeys);
		$sth = $this->prepare($sql);
		return $sth->execute($outFields);
	}

	/**
	 *! @function delete
	 *  @abstract Creates and executes an delete sql statement with mysql syntax
	 *  @param string $tabname Table name
	 *  @param array  $keys    Key values, index keys are column names
	 *
	 *  TODO: extent this method to all database types syntax
	 */
	public function delete($tabname, $keys)
	{
		$table = $this->quoteTableName($tabname);
		$whereKeys = array();
		$outFields = array();
		foreach ($keys as $key => $val) {
			switch ($this->getDriverName()) {
			case 'mysql':
				$whereKeys[] = "`${key}`=?";
				break;
			case 'pgsql':
				$whereKeys[] = "${key}=?";
				break;
			case 'mssql':
			case 'sqlsrv':
				$whereKeys[] = "[${key}]=?";
				break;
			default:
				$whereKeys[] = "${key}=?";
			}
			if (is_array($val)) {
				$outFields[] = json_encode($val);
			} else {
				$outFields[] = $val;
			}
		}
		$sql = "DELETE FROM ${table} WHERE " . implode(' AND ', $whereKeys);
		$sth = $this->prepare($sql);
		return $sth->execute($outFields);
	}

	/**
	 *! @function tableExists
	 *  @abstract Check if a table with given name exists in database
	 *  @param string $tabname Table name
	 *  @return Boolean
	 *
	 *  TODO: extent this method to all database types syntax
	 */
	public function tableExists($tabname)
	{
		$te=false;
		if(!empty($tabname)) {
            $name = trim($tabname);
			$stt = $this->prepare('SHOW TABLES');
			$stt->execute();
			while(list($tname)=$stt->fetch(PDO::FETCH_NUM)){
				if(trim($tname)==$name){
					$te=true;
					break;
				}
			}
		}
		return $te;
	}

	/**
	 * Method which describes the table columns
	 *
	 * @param string $table Table name
	 * @return array
	 *
	 **/
	public function getColumns($table)
	{
		$columns = array();
		switch ($this->getDriverName()) {
		case 'mysql':
			$stmt = parent::query('DESCRIBE `'.$table.'`');
			while($col = $stmt->fetch(PDO::FETCH_ASSOC)) {
				$colname = $col['Field'];
				$columns[$colname] = array();
				$columns[$colname]['Field'] = $col['Field'];
				$columns[$colname]['Type'] = $col['Type'];
				$columns[$colname]['Key'] = $col['Key'];
				$columns[$colname]['Default'] = $col['Default'];
			}
			break;
		case 'pgsql':
			if (!$this->catalog) {
				$this->schema = $this->_config['dsn']['dbname'];
				$stmt = parent::query("SELECT table_catalog as catalog FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '{$this->schema}' AND table_name = '${table}';");
				$row = $stmt->fetch(PDO::FETCH_ASSOC);
				$this->catalog = $row['catalog'];
			}
			$stmt = parent::query("SELECT a.attname AS Field, t.typname AS Type FROM pg_database d, pg_namespace n, pg_class c, pg_attribute a, pg_type t WHERE d.datname = '{$this->catalog}' AND n.nspname = '{$this->schema}' AND c.relname = '${table}' AND c.relnamespace = n.oid AND a.attnum > 0 AND not a.attisdropped AND a.attrelid = c.oid AND a.atttypid = t.oid ORDER BY a.attnum");
			while($col = $stmt->fetch(PDO::FETCH_ASSOC)) {
				$colname = $col['Field'];
				$columns[$colname] = array();
				$columns[$colname]['Field'] = $col['Field'];
				$columns[$colname]['Type'] = $col['Type'];
				//$columns[$colname]['Default'] = $col['Default'];
				$columns[$colname]['Default'] = '';
			}
			break;
		case 'dblib':
		case 'mssql':
		case 'sqlsrv':
			if (!$this->catalog) {
				$this->schema = $this->_config['dsn']['dbname'];
				$stmt = parent::query("SELECT table_catalog as catalog FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '{$this->schema}' AND table_name = '${table}';");
				$row = $stmt->fetch(PDO::FETCH_ASSOC);
				$this->catalog = $row['catalog'];
			}
			$stmt = parent::query("SELECT column_name AS Field, data_type AS Type, column_default AS DefaultValue FROM information_schema.columns WHERE table_catalog = '{$this->catalog}' AND table_name = '${table}';");
			while($col = $stmt->fetch(PDO::FETCH_ASSOC)) {
				$colname = $col['Field'];
				$columns[$colname] = array();
				$columns[$colname]['Field'] = $col['Field'];
				$columns[$colname]['Type'] = $col['Type'];
				$columns[$colname]['Default'] = $col['DefaultValue'];
			}
			break;
		default:
			break;
		}
		return $columns;
	}

	/**
	 *! @function getPrimaryKey
	 *  @abstract Finds fields that compose the primary key of a table
	 *  @param string $table Table name
	 *  @return String Associative Array with fieldnames as indexes
	 *
	 *  TODO: extent this method to all database types syntax
	 */
	public function getPrimaryKey($table)
	{
		$columns = $this->getColumns($table);
		$pk=array();
		reset($columns);
		while (list($colname, $col) = each($columns)) {
			if(strtolower(trim($col['Key']))=='pri') {
				$value = strtolower(trim($value = $col['Default']));
				if ($value == '(null)') {
					$value = '';
				} elseif ($value == 'null') {
					$value = '';
				}
				$pk[$colname] = $value;
			}
		}
		return $pk;
	}

	/**
	 *! @function getEmptyRow
	 *  @abstract Finds fields that compose a table row
	 *  @param string - Table name
	 *
	 *  @return String Associative Array with fieldnames as indexes
	 */
	public function getEmptyRow($table)
	{
		$er = array();
		foreach ($this->getColumns($table) as $colname => $col) {
			$value = strtolower(trim($value = $col['Default']));
			if ($value == '(null)') {
				$value = '';
			} elseif ($value == 'null') {
				$value = '';
			}
			$er[$colname] = $value;
		}
		return $er;
	}

	/**
	 * Method which describes the database tables
	 *
	 * @return array
	 **/
	public function getTables()
	{
		$tables = array();
		switch ($this->getDriverName()) {
		case 'mysql':
			$sth = parent::query('SHOW TABLES');
			while($row = $sth->fetch(PDO::FETCH_NUM)) {
				$tabname = $row[0];
				$tables[$tabname] = array();
				$tables[$tabname]['name'] = $tabname;
				$tables[$tabname]['cols'] = $this->getColumns($tabname);
			}
			break;
		case 'pgsql':
			$sth = parent::query("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '" . $this->_config['dsn']['dbname'] . "';");
			while($row = $sth->fetch(PDO::FETCH_NUM)) {
				$tabname = $row[0];
				$tables[$tabname] = array();
				$tables[$tabname]['name'] = $tabname;
				$tables[$tabname]['cols'] = $this->getColumns($tabname);
			}
			break;
		case 'dblib':
		case 'mssql':
		case 'sqlsrv':
			$sth = parent::query("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '" . $this->_config['dsn']['dbname'] . "';");
			while($row = $sth->fetch(PDO::FETCH_NUM)) {
				$tabname = $row[0];
				$tables[$tabname] = array();
				$tables[$tabname]['name'] = $tabname;
				$tables[$tabname]['cols'] = $this->getColumns($tabname);
			}
			break;
		default:
			break;
		}
		return $tables;
	}

}

//
if (!defined('__PDO_STARTED__')) {
  define('__PDO_STARTED__',1);
}

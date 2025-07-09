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
 * @copyright  2013-2015 Gustavo Adolfo D'Anetra
 * @link       http://www.gdanetra.net
 * @version    2.0.0
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
 * 1.5.5 - passwordHash()
 * 1.5.6 - Adjust for namespaces
 * 1.5.7 - sql_clause()
 * 1.5.8 - encrypt() and decrypt()
 * 1.6 - getColumns() updated
 * 1.6.1 - buildInsert() and buildReplace()
 * 1.6.2 - buildSelect()
 * 1.6.3 - camelCase()
 * 1.6.4 - select_MAX() and select_MIN()
 * 1.6.5 - executeSelect()
 * 1.6.6 - select_SUM()
 * 1.6.7 - select_COUNT()
 * 1.6.8 - executeSelectAll()
 * 1.6.9 - Permit an array of conditions in where parameters
 * 1.7 - Changes array notation to []
 * 1.7.1 - Permit an array of conditions in where parameter for update and delete
 * 2.0.0 - Change to PHP 8.x
 */
class dbPDO extends PDO
{
    /**
     * @access private
     * @var array|false
     */
    var array|false $config = [];
    /**
     * @var array
     */
    var array $implemented =  [
        'PDO_DBLIB' => true,      // FreeTDS / Microsoft SQL Server / Sybase
        'PDO_MYSQL' => true,     // MySQL 3.x/4.x/5.x
        'PDO_PGSQL' => true,      // PostgreSQL
        'PDO_SQLITE' => true,     // SQLite 3 and SQLite 2
        'PDO_SQLSRV' => false,     // Microsoft SQL Server / SQL Azure
    ];
    /**
     * @access protected
     * @var bool
     */
    protected bool $hasActiveTransaction = false;
    /**
     * @access protected
     * @var bool
     */
    protected mixed $schema = false;
    /**
     * @access protected
     * @var bool
     */
    protected mixed $catalog = false;
    /* Last Query Info */
    /**
     * @var string
     */
    var string $sql = '';
    /**
     * @var string
     */
    var string $getSql = '';
    /**
     * @var string
     */
    var string $getPage = '';
    /**
     * @var string
     */
    var string $getLimit = '';
    /**
     * @var string
     */
    var string $getTotalPages = '';
    /**
     * @var string
     */
    var string $getTotalRows = '';
    /**
     * @var string
     */
    var string $stmt = '';
    /**
     * @var array
     */
    var array $current = [];

    /**
     * Exception Handler
     *
     * Personalize for your convenience
     *
     */
    public static function exception_handler($exception): void
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
     * @param string $file the name of a file containing the parameters
     *                      or an array containing the parameters.
     *
     * These are as follows:
     * [
     *    'db_driver' => 'mysql',                       // The database driver
     *    'db_user' => 'username',                      // The user
     *    'db_password' => 'password',                  // The password
     *    'db_blowfish' => '4Af7M6cP28xwDrt5AKsMWB',    // An string used to encrypt data
     *    'dsn' => [
     *        'host' => 'localhost',                    // The database server
     *        'port' => 3306,                           // The database port
     *        'dbname' => 'example_database' ],         // The database name
     *        'charset' => 'utf8' ),                    // The database connection charset
     *    'db_options' => [
     *        '\PDO::ATTR_PERSISTENT' => 1 ],            // The options you want to use
     *    'db_attributes' => [
     *        'ATTR_ERRMODE' => 'ERRMODE_EXCEPTION' ],  // The attributes you want to set
     *    'charset' => 'utf8',                          // The SET NAMES charset
     *    'log_sql' => true,                            // True to log the sql
     *    'log_filename' => 'log.sql',                  // SQL log filename
     *    'last_sql' => true,                           // True to get last prepared SQL
     *    'last_sql_value' => 'use example_database'    // Default value for last prepared SQL
     * ];
     *
     * @throws exception
     */
    public function __construct(string $file='dbConfig.ini')
    {
        if (is_array($file)) {
            $this->config = $file;
        } else {
            if (!$this->config = parse_ini_file($file, true)) {
                throw new exception('Unable to open '.$file.'.');
            }
        }
        $dbDriver = $this->config['db_driver'];
        $dsn = $dbDriver.':';
        if (!isset($this->config['dsn']['charset'])) {
            $this->config['dsn']['charset'] = 'utf8';
        }
        if ($dbDriver == 'sqlsrv') {
            $dsn .= 'Server=' . $this->config['dsn']['host']
                .',' . $this->config['dsn']['port']
                .';Database='.$this->config['dsn']['dbname'];
        } else {
            foreach ($this->config['dsn'] as $k => $v) {
                $dsn .= "{$k}={$v};";
            }
        }
        // Temporarily change the PHP exception handler while we . . .
        set_exception_handler([__CLASS__, 'exception_handler']);
        parent::__construct($dsn, $this->config['db_user'], $this->config['db_password'], $this->config['db_options']);
        foreach ($this->config['db_attributes'] as $k => $v) {
            parent::setAttribute(constant("\PDO::{$k}"), constant("\PDO::{$v}"));
        }

        // Set table schema
        $this->schema = $this->config['dsn']['dbname'];
        // Set table catalog for pgsql, mssql and dblib drivers
        switch ($dbDriver) {
            case 'dblib':
            case 'pgsql':
            case 'mssql':
            case 'sqlsrv':
                $stmt = parent::query("SELECT table_catalog as catalog FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '{$this->schema}';");
                $row = $stmt->fetch(\PDO::FETCH_ASSOC);
                $this->catalog = $row['catalog'];
                break;
            case 'mysql':
                $stmt = parent::prepare("SET GLOBAL SQL_MODE='ALLOW_INVALID_DATES'");
                $stmt->execute();
                break;
            default:
                throw new \UnexpectedValueException('PDO driver "' . $dbDriver . '" not supported by dbPDO');
        }
        // Set charset.
        if (isset($this->config['charset'])) {
            switch ($dbDriver) {
                case 'dblib':
                case 'mssql':
                case 'sqlsrv':
                    break;
                case 'mysql':
                case 'pgsql':
                    parent::prepare("SET NAMES '" . $this->config['charset'] . "'")->execute();
                    break;
                default:
                    throw new \UnexpectedValueException('PDO driver "' . $dbDriver . '" not supported by dbPDO');
            }
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
        return $this->config['dsn']['dbname'];
    }

    /**
     * To encapsulate tabnames
     *
     * @access protected
     */
    protected function quoteTableName($table, $quote=null): string
    {
        $driver = $this->getDriverName();
        if (null === $quote) {
            $quotes = match ($driver) {
                'mysql' => ['`', '`'],
                'pgsql' => ['', ''],
                'dblib' => ['[', ']'],
                'mssql', 'sqlsrv' => ['"', '"'],
                default => ['', ''],
            };
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
    protected function fieldAlias(string $field): string
    {
        if ($p = strpos($field, '.')) {
            $field = substr($field,$p+1);
        }
        return preg_replace('/ /', '_', $field);
    }

    /**
     * To enable/disable the log file
     */
    public function setLog($log_filename='log.sql'): void
    {
        $this->config['log_sql'] = true;
        $this->config['log_filename'] = $log_filename;
    }

    /**
     *
     */
    public function unsetLog(): void
    {
        $this->config['log_sql'] = false;
    }

    /**
     * To enable/disable the last sql option
     */
    public function setLastSQL($last_sql_value='use'): void
    {
        $this->config['last_sql'] = true;
        $this->config['last_sql_value'] = $last_sql_value;
    }

    /**
     *
     */
    public function unsetLastSQL(): void
    {
        $this->config['last_sql'] = false;
    }

    /**
     * set quotes (',",`) for an unquoted string
     *
     * @param string $string string to quote
     * @param   string  $type type of value
     * @return  false|string  quoted string
     */
    public function quote(string $string, $type=PDO::PARAM_STR ): false|string
    {
        if ( is_null($string) ) {
            return 'NULL';
        }
        return parent::quote($string, $type);
    }

    /**
     * removes quotes (',",`) from a quoted string
     *
     * checks if the sting is quoted and removes this quotes
     *
     * @param string $quoted_string  string to remove quotes from
     * @param string|null $quote          type of quote to remove
     * @return  string  unquoted string
     */
    function unQuote(string $quoted_string, ?string $quote=null): string
    {
        $quotes = [];
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
                return str_replace($quote.$quote, $quote, $unquoted_string);
            }
        }
        return $quoted_string;
    }

    /**
     *! @function sql_clause.
     *
     * @abstract Construct an sql statement
     *
     * @param string $table The table name
     * @param string $cols The selected columns
     * @param array $fields The key fields
     * @param bool | array $where The where options
     * @param bool | array  $orderby The order by options
     * @param bool | integer  $limit The row limit
     *
     * @return array|false|string|null
     *
     * @since 1.5.7
     */
    function sql_clause($table, $cols='*', $fields=[], $where=false, $orderby=false, $limit=false): array|false|string|null
    {
        return $this->buildSelect($table,$cols,$fields,$where,$orderby,$limit);
    }

    /**
     *! @function buildSelect.
     *
     *  @abstract Construct an sql statement
     *
     * @since 1.6.2
     *
     * @param      $table
     * @param      $cols
     * @param      $fields
     * @param bool $where
     * @param bool $orderby
     * @param bool $limit
     *
     * @return bool|string
     */
    public function buildSelect($table,$cols='*',$fields=[],$where=false,$orderby=false,$limit=false)
    {
        if (!is_array($fields)) {
            return false;
        }
        if (is_array($cols)) {
            $cols = implode(',',$cols);
        }
        if (is_array($where)) {
            $where = implode(' AND ',$where);
        }
        $tabname = $this->quoteTableName($table);
        $driver = $this->getDriverName();
        if (!is_array($cols)) {
            if ($cols === 'map') {
                $cols = $this->getColumns($table);
                $sep = '';
                $columns = '';
                foreach ($cols as $key => $col) {
                    $columns .= $sep;
                    $val = $col['Map'];
                    $columns .= match ($driver) {
                        'mysql' => " `{$key}`",
                        'pgsql' => "{$key}",
                        'dblib' => "[{$key}]",
                        'mssql', 'sqlsrv' => "[{$key}]",
                        default => "{$key}",
                    };
                    if ($val === '') {
                        $val = $key;
                    } elseif ($val === null) {
                        $val = $key;
                    }
                    if ($key !== $val) {
                        $columns .= ' as '.$val;
                    }
                    $sep = ',';
                }
            } else {
                $columns = $cols;
            }
        } else {
            $sep = '';
            $columns = '';
            foreach ($cols as $key => $val) {
                $columns .= $sep;
                $columns .= match ($driver) {
                    'mysql' => " `{$key}`",
                    'pgsql' => "{$key}",
                    'dblib', 'mssql', 'sqlsrv' => "[{$key}]",
                    default => "{$key}",
                };
                if ($val === '') {
                    $val = $key;
                } elseif ($val === null) {
                    $val = $key;
                }
                if ($key !== $val) {
                    $columns .= ' as '.preg_replace('/ /', '_', preg_replace('/-/', '', $val));
                }
                $sep = ',';
            }
        }
        $sql = "SELECT {$columns} FROM {$tabname}";
        if ($where === null) {
            $sep = 'WHERE';
        } elseif ($where === false) {
            $sep = 'WHERE';
        } else {
            // @todo where array
            $sql .= ' WHERE ' . $where;
            $sep = 'AND';
        }
        foreach ($fields as $key => $val) {
            $field = match ($driver) {
                'mysql' => "`{$key}`",
                'pgsql' => "{$key}",
                'dblib', 'mssql', 'sqlsrv' => "[{$key}]",
                default => "{$key}",
            };
            if (strpos($val, '%')) {
                $sql .= " {$sep} {$field} LIKE :{$key}";
            } else {
                $sql .= " {$sep} {$field} = :{$key}";
            }
            $sep = 'AND';
        }
        if ($limit) {
            switch ($driver) {
                case 'mysql':
                    if ($orderby) {
                        $sql .= " ORDER BY {$orderby}";
                    }
                    $sql .= " LIMIT {$limit}";
                    break;
                case 'pgsql':
                    if ($orderby) {
                        $sql .= " ORDER BY {$orderby}";
                    }
                    if (strpos(',', $limit)) {
                        list($start, $count) = explode(',', $limit);
                        $sql .= " LIMIT {$limit} OFFSET {$start}";
                    } else {
                        $sql .= " LIMIT {$limit}";
                    }
                    break;
                case 'dblib':
                case 'mssql':
                case 'sqlsrv':
                    if (strpos(',', $limit)) {
                        if ($orderby) {
                            if (strpos(',', $orderby)) {
                                $orderFields = explode(',', $orderby);
                                $rorderFields = [];
                                foreach ($orderFields as  $k => $field) {
                                    if (strpos(' asc', $orderby)) {
                                        $rorderFields[] = preg_replace('/ asc/', ' desc', $field);
                                    } elseif (strpos(' desc', $orderby)) {
                                        $rorderFields[] = preg_replace('/ desc/', ' asc', $field);
                                    } elseif (strpos(' ASC', $orderby)) {
                                        $rorderFields[] = preg_replace('/ ASC/', ' desc', $field);
                                    } elseif (strpos(' DESC', $orderby)) {
                                        $rorderFields[] = preg_replace('/ DESC/', ' asc', $field);
                                    } else {
                                        $orderFields[$k] = $field.' asc';
                                        $rorderFields[] = $field.' desc';
                                    }
                                }
                                $orderby = ' ORDER BY '.implode(',', $orderFields);
                                $rorderby = ' ORDER BY '.implode(',', $rorderFields);
                            } elseif (strpos(' asc', $orderby)) {
                                $orderby = " ORDER BY {$orderby}";
                                $rorderby = preg_replace('/ asc/', ' desc', $orderby);
                            } elseif (strpos(' desc', $orderby)) {
                                $orderby = " ORDER BY {$orderby}";
                                $rorderby = preg_replace('/ desc/', ' asc', $orderby);
                            } elseif (strpos(' ASC', $orderby)) {
                                $orderby = " ORDER BY {$orderby}";
                                $rorderby = preg_replace('/ ASC/', ' desc', $orderby);
                            } elseif (strpos(' DESC', $orderby)) {
                                $orderby = " ORDER BY {$orderby}";
                                $rorderby = preg_replace('/ DESC/', ' asc', $orderby);
                            } else {
                                $rorderby = ' ORDER BY '.$orderby;
                                $orderby = $rorderby.' asc';
                                $rorderby = $rorderby.' desc';
                            }
                        } else {
                            $orderby = ' ORDER BY 1 asc';
                            $rorderby = ' ORDER BY 1 desc';
                        }
                        list($start, $count) = explode(',', $limit);
                        $top = intval($start) + intval($count);
                        $total = $this->executeRecordCount($sql, $fields);
                        if ($top > $total) {
                            $top = $total;
                            $count = $top - $start;
                        }
                        $sql = preg_replace('/SELECT /', "SELECT * FROM (SELECT TOP {$count} * FROM (SELECT TOP {$top} ", $sql)."{$orderby}) AS NewTable1 {$rorderby}) AS NewTable2 {$orderby}";
                    } else {
                        $sql = preg_replace('/SELECT /', "SELECT TOP {$limit} ", $sql);
                        if ($orderby) {
                            $sql .= " ORDER BY {$orderby}";
                        }
                    }
                    break;
                default:
                    if ($orderby) {
                        $sql .= " ORDER BY {$orderby}";
                    }
                    $sql .= " LIMIT {$limit}";
                    break;
            }
        } elseif ($orderby) {
            $sql .= " ORDER BY {$orderby}";
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
        return $this->executeSelect($table, '*', $fields, $where, $orderby, $limit);
    }

    /**
     * Select Table record(s)
     * Returns a SQL stament result.
     */
    public function executeSelect($table,$cols,$fields=[],$where=false,$orderby=false,$limit=false)
    {
        if (!is_array($fields)) {
            return false;
        }
        if (is_array($cols)) {
            $cols = implode(',',$cols);
        }
        $this->sql = $this->buildSelect($table,$cols,$fields,$where,$orderby,$limit);
        $this->stmt = $this->prepare($this->sql);
        $this->stmt->execute($fields);

        return $this->stmt->fetch(\PDO::FETCH_ASSOC);
    }

    /**
     * Select All Table record(s)
     * Returns a SQL stament result.
     */
    public function executeSelectAll($table,$fields=[],$where=false,$orderby=false,$limit=false)
    {
        return $this->executeSelect($table,'*',$fields,$where,$orderby,$limit);
    }

    /**
     *! @function select_MAX.
     *
     *  @abstract Construct a sql statement to get the MAX(column)
     *
     * @since 1.6.4
     *
     * @param string      $table
     * @param string      $column
     * @param array       $fields
     * @param bool|string $where
     * @param bool|string $orderby
     * @param bool|string $limit
     *
     * @return bool|string
     */
    public function select_MAX($table,$column,$fields=[],$where=false,$orderby=false,$limit=false)
    {
        if (!is_array($fields)) {
            $fields = [];
        }
        $sql = $this->buildSelect($table,"MAX($column) as mxmum",$fields,$where,$orderby,$limit);
        $sth = $this->prepare($sql);
        if (count($fields) > 0) {
            $sth->execute($fields);
        } else {
            $sth->execute();
        }
        if ($row = $sth->fetch(\PDO::FETCH_ASSOC)) {
            return $row['mxmum'];
        }
        return false;
    }

    /**
     *! @function select_MIN.
     *
     *  @abstract Construct a sql statement to get the MIN(column)
     *
     * @since 1.6.4
     *
     * @param string      $table
     * @param string      $column
     * @param array       $fields
     * @param bool|string $where
     * @param bool|string $orderby
     * @param bool|string $limit
     *
     * @return bool|string
     */
    public function select_MIN($table,$column,$fields=[],$where=false,$orderby=false,$limit=false)
    {
        if (!is_array($fields)) {
            $fields = [];
        }
        $sql = $this->buildSelect($table,"MIN($column) as mnmum",$fields,$where,$orderby,$limit);
        $sth = $this->prepare($sql);
        $sth->execute($fields);
        if ($row = $sth->fetch(\PDO::FETCH_ASSOC)) {
            return $row['mnmum'];
        }
        return false;
    }

    /**
     *! @function select_SUM.
     *
     *  @abstract Construct a sql statement to get the SUM(column)
     *
     * @since 1.6.6
     *
     * @param string      $table
     * @param string      $column
     * @param array       $fields
     * @param bool|string $where
     * @param bool|string $orderby
     * @param bool|string $limit
     *
     * @return bool|string
     */
    public function select_SUM($table,$column,$fields=[],$where=false,$orderby=false,$limit=false)
    {
        if (!is_array($fields)) {
            $fields = [];
        }
        $sql = $this->buildSelect($table,"SUM($column) as ttal",$fields,$where,$orderby,$limit);
        $sth = $this->prepare($sql);
        $sth->execute($fields);
        if ($row = $sth->fetch(\PDO::FETCH_ASSOC)) {
            return $row['ttal'];
        }
        return 0.0;
    }

    /**
     *! @function select_COUNT.
     *
     *  @abstract Construct a sql statement to get the COUNT(column)
     *
     * @since 1.6.7
     *
     * @param string      $table
     * @param string      $column
     * @param array       $fields
     * @param bool|string $where
     * @param bool|string $orderby
     * @param bool|string $limit
     *
     * @return bool|string
     */
    public function select_COUNT($table,$column,$fields=[],$where=false,$orderby=false,$limit=false)
    {
        if (!is_array($fields)) {
            $fields = [];
        }
        $sql = $this->buildSelect($table,"COUNT($column) as ttal",$fields,$where,$orderby,$limit);
        $sth = $this->prepare($sql);
        $sth->execute($fields);
        if ($row = $sth->fetch(\PDO::FETCH_ASSOC)) {
            return $row['ttal'];
        }
        return 0.0;
    }

    /**
     * Select Table record(s)
     * Returns a SQL statement result
     */
    function get_records_page($table, $fields, $page, $limit=20, $sidx=1, $sord='asc')
    {
        $tabname = $this->quoteTableName($table);
        $this->getTotalRows = $this->select_COUNT($tabname,'*');
        $this->getLimit = $limit;
        $this->getPage = $page;
        if ($this->getTotalRows > 0) {
            $this->getTotalPages = ceil($this->getTotalRows / $this->getLimit);
        } else {
            $this->getTotalPages = 0;
        }
        if ($this->getPage > $this->getTotalPages) {
            $this->getPage = $this->getTotalPages;
        }
        $start = ($this->getLimit * $this->getPage) - $this->getLimit;
        if ($start < 0) {
            $start = 0;
        }

        $sql = $this->buildSelect($table,'*',$fields,false,"{$sidx} {$sord}","{$start},{$limit}");
        $page = [];
        $stp = $this->prepare($sql);
        $stp->execute($fields);
        while ($row = $stp->fetch(\PDO::FETCH_ASSOC)) {
            $page[] = $row;
        }
        return $page;
    }

    /**
     * Returns the record count from a query
     */
    public function queryRecordCount($sqlClause)
    {
        $sqlClause = trim($sqlClause);
        $sth = parent::query("SELECT count(*) as RecordCount FROM ({$sqlClause}) as CntTbl");
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
        $sth = $this->prepare("SELECT count(*) as RecordCount FROM ({$sqlClause}) as CntTbl");
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
        return $this->config['db_driver'];
    }

    /**
     * 1. Log the SQL clause
     * 2. Save the SQL clause
     * 3. Prepare the clause
     */
    public function prepare($query, $options=[]): false|PDOStatement
    {
        if ($this->config['log_sql']) {
            $logfile = fopen($this->config['log_filename'], 'a');
            fwrite($logfile, "{$query}\n");
            fclose($logfile);
        }
        if ($this->config['last_sql']) {
            $this->config['last_sql_value'] = $query;
        }
        return parent::prepare($query, $options);
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
    public function fetchNum($stmt): mixed
    {
        return $stmt->fetch(PDO::FETCH_NUM);
    }

    /**
     *
     * @access private
     * @return string
     */
    private function getBlowfishKey(): string
    {
        return '$2a$'.$this->config['db_blowfish'];
    }

    /**
     * @param string $v
     * @return string
     */
    public function getCrypt(string $v=''): string
    {
        return crypt($v, $this->getBlowfishKey());
    }

    /**
     * AES encryption.
     *
     * @param $value
     *
     * @return string
     *
     * TODO: extent this method to all database types syntax
     */
    public function aesEncrypt($value): string
    {
        $data = [
            'value' => $value,
            'salt' => $this->getBlowfishKey(),
        ];
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
    public function aesDecrypt($value): mixed
    {
        $data = [
            'value' => base64_decode($value),
            'salt' => $this->getBlowfishKey(),
        ];
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
        if ($this->config['last_sql']) {
            return false;
        }
        return $this->config['last_sql_value'];
    }

    /**
     * Simple encryption.
     *
     * @param $value
     *
     * @return string
     *
     */
    public function encrypt($value)
    {
        $salt = $this->getBlowfishKey();
        return rawurlencode(base64_encode(openssl_encrypt($value,'AES-256-CBC', md5($salt))));
        //return rawurlencode(base64_encode(mcrypt_encrypt(MCRYPT_RIJNDAEL_256, md5($salt), $string, MCRYPT_MODE_CBC, md5(md5($salt)))));
    }


    /**
     * @param $value
     *
     * @return mixed
     *
     */
    public function decrypt($value)
    {
        $salt = $this->getBlowfishKey();
        return rawurlencode(base64_encode(openssl_decrypt($value,'AES-256-CBC', md5($salt))));
        //return rawurldecode(rtrim(mcrypt_decrypt(MCRYPT_RIJNDAEL_256, md5($salt), base64_decode(rawurldecode($string)), MCRYPT_MODE_CBC, md5(md5($salt))), "\0"));
    }

    /**
     *! @function beginTransaction
     *  @abstract Checks if a transaction has been already active or begins a new
     *            transaction
     */
    function beginTransaction(): bool
    {
        if ( !$this->inTransaction() ) {
            if ($this->hasActiveTransaction = parent::beginTransaction ()) {
                if ($this->config['log_sql']) {
                    $logfile = fopen($this->config['log_filename'], "a");
                    fwrite($logfile, ">>> BEGIN TRANSACTION\n");
                    fclose($logfile);
                }
            }
        }
        return $this->hasActiveTransaction;
    }

    /**
     *! @function commit
     *  @abstract Checks if a transaction has been already active a commits it
     */
    function commit(): bool
    {
        if ( $this->inTransaction() ) {
            parent::commit ();
            if ($this->config['log_sql']) {
                $logfile = fopen($this->config['log_filename'], "a");
                fwrite($logfile, "<<< COMMIT TRANSACTION\n");
                fclose($logfile);
            }
            $this->hasActiveTransaction = false;
        }
        return true;
    }

    /**
     *! @function rollback
     *  @abstract Checks if a transaction has been already active a commits it
     */
    function rollback(): bool
    {
        if ( $this->inTransaction() ) {
            parent::rollback ();
            if ($this->config['log_sql']) {
                $logfile = fopen($this->config['log_filename'], "a");
                fwrite($logfile, "<<< ROLLBACK TRANSACTION\n");
                fclose($logfile);
            }
            $this->hasActiveTransaction = false;
        }
        return true;
    }

    /**
     *! @function optimizeDataBase
     *  @abstract Executed upon destruction of class instance to perform
     *            repair, optimize and flush commands on each table in database
     */
    public function optimizeDataBase(): void
    {
        switch ($this->getDriverName()) {
            case 'mysql':
                $dbTables = 'Tables_in_'.$this->config['dsn']['dbname'];
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
     *  @param array $data    Row data values, index keys are column names
     *
     *  TODO: extent this method to all database types syntax
     */
    public function insert(string $tabname, array $data): bool
    {
        $sth = $this->prepare($this->buildInsert($tabname,$data));
        $outFields = [];
        foreach ($data as $key => $val) {
            if (is_array($val)) {
                $outFields[$key] = json_encode($val);
            } else {
                $outFields[$key] = $val;
            }
        }
        return $sth->execute($outFields);
    }

    /**
     *! @function buildInsert.
     *
     *  @abstract Creates an insert sql statement with for mysql, pgsql, dblib, mssql and sqlsrv drivers.
     *
     * @param string $tabname Table name
     *  @param array  $data   Row data values, index keys are column names
     *  @param string $action Database action
     *
     * @return string
     *
     *  TODO: extent this method to all database types syntax
     *@since 1.6.1
     *
     */
    public function buildInsert($tabname, $data, $action='INSERT')
    {
        $table = $this->quoteTableName($tabname);
        $fieldvalues = ':'.implode(', :', array_keys($data));
        $fieldnames = match ($this->getDriverName()) {
            'mysql' => '`' . implode('`, `', array_keys($data)) . '`',
            'pgsql' => implode(',', array_keys($data)),
            'dblib', 'mssql', 'sqlsrv' => '[' . implode('], [', array_keys($data)) . ']',
            default => implode(',', array_keys($data)),
        };
        $sql = "{$action} INTO {$table} ({$fieldnames}) VALUES ({$fieldvalues})";
        return $sql;
    }

    /**
     *! @function replace
     *
     *  @param string $tabname Table name
     *  @param array  $data    Row data values, index keys are column names
     *
     *  TODO: extent this method to all database types syntax
     */
    public function replace($tabname, $data): bool
    {
        $sth = $this->prepare($this->buildReplace($tabname,$data));
        $outFields = [];
        foreach ($data as $key => $val) {
            if (is_array($val)) {
                $outFields[$key] = json_encode($val);
            } else {
                $outFields[$key] = $val;
            }
        }
        return $sth->execute($outFields);
    }

    /**
     *! @function buildReplace.
     *
     * @abstract Creates a replacement sql statement for
     *           mysql, pgsql, dblib, mssql and sqlsrv drives.
     *
     * @since 1.6.1
     *
     * @param string $tabname Table name
     * @param array  $data    Row data values, index keys are column names
     *
     * @return string
     *
     * @todo: extent this method to all database types syntax
     */
    public function buildReplace($tabname, $data)
    {
        return $this->buildInsert($tabname,$data,'REPLACE');
    }

    /**
     *! @function update
     *  @abstract Creates and executes an insert sql statement with mysql syntax
     *  @param string $tabname Table name
     *  @param array  $data    Row data values, index keys are column names
     *  @param array  $keys    Key values, index keys are column names
     * @param mixed $whereKeys false | array of conditions
     *
     *  TODO: extent this method to all database types syntax
     */
    public function update($tabname, $data, $keys, $whereKeys=false)
    {
        $table = $this->quoteTableName($tabname);
        $setFields = [];
        $outFields = [];
        foreach ($data as $key => $val) {
            $setFields[] = match ($this->getDriverName()) {
                'mysql' => "`{$key}`=?",
                'pgsql' => "{$key}=?",
                'dblib', 'mssql', 'sqlsrv' => "[{$key}]=?",
                default => "{$key}=?",
            };
            if (is_array($val)) {
                $outFields[] = json_encode($val);
            } else {
                $outFields[] = $val;
            }
        }
        if (!is_array($whereKeys)) {
            $whereKeys = [];
        }
        foreach ($keys as $key => $val) {
            $whereKeys[] = match ($this->getDriverName()) {
                'mysql' => "`{$key}`=?",
                'pgsql' => "{$key}=?",
                'dblib', 'mssql', 'sqlsrv' => "[{$key}]=?",
                default => "{$key}=?",
            };
            if (is_array($val)) {
                $outFields[] = json_encode($val);
            } else {
                $outFields[] = addslashes($val);
            }
        }
        $sql = "UPDATE {$table} SET " . implode(',', $setFields) . ' WHERE ' . implode(' AND ', $whereKeys);
        $sth = $this->prepare($sql);
        return $sth->execute($outFields);
    }

    /**
     *! @function delete
     *  @abstract Creates and executes a delete sql statement with mysql syntax
     *  @param string $tabname Table name
     *  @param array  $keys    Key values, index keys are column names
     * @param mixed $whereKeys false | array of conditions
     *
     *  TODO: extent this method to all database types syntax
     */
    public function delete($tabname, $keys, $whereKeys=false)
    {
        $table = $this->quoteTableName($tabname);
        if (!is_array($whereKeys)) {
            $whereKeys = [];
        }
        $outFields = [];
        foreach ($keys as $key => $val) {
            $whereKeys[] = match ($this->getDriverName()) {
                'mysql' => "`{$key}`=?",
                'pgsql' => "{$key}=?",
                'mssql', 'sqlsrv' => "[{$key}]=?",
                default => "{$key}=?",
            };
            if (is_array($val)) {
                $outFields[] = json_encode($val);
            } else {
                $outFields[] = $val;
            }
        }
        $sql = "DELETE FROM {$table} WHERE " . implode(' AND ', $whereKeys);
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
        if (!empty($tabname)) {
            $name = trim($tabname);
            $stt = $this->prepare('SHOW TABLES');
            $stt->execute();
            while(list($tname)=$stt->fetch(PDO::FETCH_NUM)){
                if(trim($tname)==$name){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Method which describes the table columns
     *
     * @param string $table Table name
     * @param string $format Return Format (Default: 'array')
     * @return array|string
     *
     **/
    public function getColumns($table, $format=null)
    {
        $columns = [];
        switch ($this->getDriverName()) {
            case 'mysql':
                $order = 0;
                $stmt = parent::query("DESCRIBE `{$table}`");
                while ($col = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                    $colname = $col['Field'];
                    if ($col['Default'] == 'null') $col['Default']=null;
                    $order++;
                    $data = [
                        'Field' => $colname,
                        'Type' => $col['Type'],
                        'Key' => $col['Key'],
                        'Default' => $col['Default'],
                        'Map' => preg_replace('/ /', '_', preg_replace('/-/', '', $colname)),
                        'Order' => $order,
                        'Required' => true,
                    ];
                    if (is_null($col['Default']) && $col['Null'] == 'YES') $data['Required'] = false;
                    $columns[$colname] = $data;
                }
                break;
            case 'pgsql':
                if (!$this->catalog) {
                    $this->schema = $this->config['dsn']['dbname'];
                    $stmt = parent::query("SELECT table_catalog as catalog FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '{$this->schema}' AND table_name = '{$table}';");
                    $row = $stmt->fetch(\PDO::FETCH_ASSOC);
                    $this->catalog = $row['catalog'];
                }
                $order = 0;
                $stmt = parent::query("SELECT a.attname AS Field, t.typname AS Type FROM pg_database d, pg_namespace n, pg_class c, pg_attribute a, pg_type t WHERE d.datname = '{$this->catalog}' AND n.nspname = '{$this->schema}' AND c.relname = '{$table}' AND c.relnamespace = n.oid AND a.attnum > 0 AND not a.attisdropped AND a.attrelid = c.oid AND a.atttypid = t.oid ORDER BY a.attnum");
                while ($col = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                    $colname = $col['Field'];
                    $order++;
                    $data = [
                        'Field' => $colname,
                        'Type' => $col['Type'],
                        'Key' => $col['Key'],
                        'Default' => '',
                        'Map' => preg_replace('/ /', '_', preg_replace('/-/', '', $colname)),
                        'Order' => $order,
                        'Required' => true,
                    ];
                    $columns[$colname] = $data;
                }
                break;
            case 'dblib':
            case 'mssql':
            case 'sqlsrv':
            if (!$this->catalog) {
                $this->schema = $this->config['dsn']['dbname'];
                $stmt = parent::query("SELECT table_catalog as catalog FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '{$this->schema}' AND table_name = '{$table}';");
                $row = $stmt->fetch(\PDO::FETCH_ASSOC);
                $this->catalog = $row['catalog'];
            }
            //$stmt = parent::query("SELECT column_name AS Field, data_type AS Type, column_default AS DefaultValue FROM information_schema.columns WHERE table_catalog = '{$this->catalog}' AND table_name = '{$table}';");
            $stmt = parent::query("SELECT COLUMN_NAME AS Field, DATA_TYPE AS Type, is_nullable as [Null], '' as [Key], column_default AS DefaultValue, '' as Extra, ORDINAL_POSITION AS [Order]
  FROM information_schema.columns WHERE TABLE_CATALOG = '{$this->catalog}' AND TABLE_NAME = '{$table}'
  AND NUMERIC_PRECISION IS NULL AND CHARACTER_MAXIMUM_LENGTH IS NULL
  UNION
  SELECT COLUMN_NAME AS Field, DATA_TYPE+'('+CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR)+')' AS Type, is_nullable as [Null], '' as [Key], column_default AS DefaultValue, '' as Extra, ORDINAL_POSITION AS [Order]
  FROM information_schema.columns WHERE TABLE_CATALOG = '{$this->catalog}' AND TABLE_NAME = '{$table}'
  AND NUMERIC_PRECISION IS NULL AND CHARACTER_MAXIMUM_LENGTH > 0
  UNION
  SELECT COLUMN_NAME AS Field, DATA_TYPE+'(max)' AS Type, is_nullable as [Null], '' as [Key], column_default AS DefaultValue, '' as Extra, ORDINAL_POSITION AS [Order]
  FROM information_schema.columns WHERE TABLE_CATALOG = '{$this->catalog}' AND TABLE_NAME = '{$table}'
  AND NUMERIC_PRECISION IS NULL AND CHARACTER_MAXIMUM_LENGTH = -1
  UNION
  SELECT COLUMN_NAME AS Field, DATA_TYPE+'('+CAST(NUMERIC_PRECISION AS VARCHAR)+')' AS Type, is_nullable as [Null], '' as [Key], column_default AS DefaultValue, '' as Extra, ORDINAL_POSITION AS [Order]
  FROM information_schema.columns WHERE TABLE_CATALOG = '{$this->catalog}' AND TABLE_NAME = '{$table}'
  AND NUMERIC_PRECISION > 0 AND NUMERIC_SCALE = 0 AND CHARACTER_MAXIMUM_LENGTH IS NULL
  UNION
  SELECT COLUMN_NAME AS Field, DATA_TYPE+'('+CAST(NUMERIC_PRECISION AS VARCHAR)+','+CAST(NUMERIC_SCALE AS VARCHAR)+')' AS Type, is_nullable as [Null], '' as [Key], column_default AS DefaultValue, '' as Extra, ORDINAL_POSITION AS [Order]
  FROM information_schema.columns WHERE TABLE_CATALOG = '{$this->catalog}' AND TABLE_NAME = '{$table}'
  AND NUMERIC_PRECISION > 0 AND NUMERIC_SCALE > 0 AND CHARACTER_MAXIMUM_LENGTH IS NULL
  ORDER BY ORDINAL_POSITION
  ;");
            while ($col = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                $colname = $col['Field'];
                $data = [
                    'Field' => $colname,
                    'Type' => $col['Type'],
                    'Key' => $col['Key'],
                    'Default' => $col['Default'],
                    'Map' => preg_replace('/ /', '_', preg_replace('/-/', '', $colname)),
                    'Order' => $col['Order'],
                    'Required' => true,
                ];
                $columns[$colname] = $data;
            }
                break;
            default:
                break;
        }

        switch ($format) {
            case 'yaml':
                $field = $this->camelCase($table);
                $out = $field . ":\n  Type: \"object\"\n";
                if ($table !== $field) {
                    $out .= "  xml:\n";
                    $out .= "    Name: \"${table}\"\n";
                }
                // Required fields
                $txt = "  Required:\n";
                $col = [];
                foreach ($columns as $colname => $data) {
                    switch ($data['Key']) {
                        case 'PRI':
                        case 'MUL':
                            $field = $this->camelCase($data['Field']);
                            $txt .= "  - ${field}\n";
                            $col[$field] = $data;
                            break;
                        default:
                            if ($data['Required']) {
                                $field = $this->camelCase($data['Field']);
                                $txt .= "  - ${field}\n";
                                $col[$field] = $data;
                            }
                    }
                }
                if (count($col) > 0) {
                    $out .= $txt;
                }
                // Fields
                $out .= "  Properties:\n";
                foreach ($columns as $colname => $data) {
                    //$field = $this->camelCase($data['Field']);
                    $field = $this->camelCase($data['Field']);
                    $out .= "    ${field}:\n";
                    $format = strtolower($data['Type']);
                    $pos = strpos($format,'(');
                    if ($pos > 0) {
                        $size = preg_replace('/\)/','',preg_replace('/\(/','',substr($format,$pos)));
                        $type = substr($format,0,$pos);
                    } else {
                        $size = 0;
                        $type = $format;
                    }
                    switch ($type) {
                        case 'tinyint':
                            $out .= "      type: \"integer\"\n      format: \"int8\"\n";
                            break;
                        case 'smallint':
                            $out .= "      type: \"integer\"\n      format: \"int16\"\n";
                            break;
                        case 'int':
                            $out .= "      type: \"integer\"\n      format: \"int32\"\n";
                            break;
                        case 'bigint':
                            $out .= "      type: \"integer\"\n      format: \"int64\"\n";
                            break;
                        case 'date':
                        case 'datetime':
                        case 'time':
                        case 'year':
                        case 'timestamp':
                            $out .= "      type: \"string\"\n      format: \"${format}\"\n";
                            break;
                        case 'double':
                        case 'float':
                            $out .= "      type: \"numeric\"\n      format: \"${format}\"\n";
                            break;
                        case 'decimal':
                            $out .= "      type: \"numeric\"\n      format: \"${type}\"\n      dimension: \"${size}\"\n";
                            break;
                        case 'char':
                        case 'varchar':
                        case 'tinytext':
                        case 'text':
                        case 'mediumtext':
                        case 'longtext':
                        case 'json':
                            $out .= "      type: \"string\"\n      format: \"${format}\"\n";
                            break;
                        case 'enum':
                        case 'set':
                            $out .= "      type: \"string\"\n      ${type}:\n";
                            $col = preg_replace('/\)/','',preg_replace('/enum\(/','',$format));
                            $col = preg_replace('/\'/','"',$col);
                            $arr = explode(',',$col);
                            foreach ($arr as $txt) {
                                $out .= "      - $txt\n";
                            }
                            break;
                        default:
                            $out .= "      type: \"${type}\"\n      format: \"${format}\"\n";
                    }
                    if (!is_null($data['Default'])) {
                        $out .= "      default: \"" . $data['Default'] . "\"\n";
                    }
                    if ($data['Field'] !== $field) {
                        $out .= "      name: \"" . $data['Field'] . "\"\n";
                    }
                    if ($data['Map'] !== $data['Field']) {
                        $out .= "      map: \"" . $data['Map'] . "\"\n";
                    }

                }
                return $out;
                break;
            case 'migration':
                return json_encode($columns);
                break;
            case 'seed':
                return json_encode($columns);
                break;
            case 'json':
                return json_encode($columns);
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
     *  @return Array Associative Array with fieldnames as indexes
     *
     *  TODO: extent this method to all database types syntax
     */
    public function getPrimaryKey($table)
    {
        $columns = $this->getColumns($table);
        $pk=[];
        reset($columns);
        foreach ($columns as $colname => $col) {
            //while (list($colname, $col) = each($columns)) {
            if(strtolower(trim($col['Key']))=='pri') {
                $value = strtolower(trim($col['Default']));
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
     *  @param string $table Table name
     *
     *  @return Array Associative Array with fieldnames as indexes
     */
    public function getEmptyRow($table)
    {
        $er = [];
        foreach ($this->getColumns($table) as $colname => $col) {
            $value = strtolower(trim($col['Default']));
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
        $tables = [];
        switch ($this->getDriverName()) {
            case 'mysql':
                $sth = parent::query('SHOW TABLES');
                while($row = $sth->fetch(PDO::FETCH_NUM)) {
                    $tabname = $row[0];
                    $tables[$tabname] = [
                        'name' => $tabname,
                        'cols' => $this->getColumns($tabname)
                    ];
                }
                break;
            case 'pgsql':
                $sth = parent::query("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '" . $this->config['dsn']['dbname'] . "';");
                while($row = $sth->fetch(PDO::FETCH_NUM)) {
                    $tabname = $row[0];
                    $tables[$tabname] = [
                        'name' => $tabname,
                        'cols' => $this->getColumns($tabname)
                    ];
                }
                break;
            case 'dblib':
            case 'mssql':
            case 'sqlsrv':
                $sth = parent::query("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '" . $this->config['dsn']['dbname'] . "';");
                while($row = $sth->fetch(PDO::FETCH_NUM)) {
                    $tabname = $row[0];
                    $tables[$tabname] = [
                        'name' => $tabname,
                        'cols' => $this->getColumns($tabname)
                    ];
                }
                break;
            default:
                break;
        }
        return $tables;
    }

    /**
     * Password hash.
     *
     * @since 1.5.5
     *
     * @param        $pass
     * @param string $hash
     * @param string $salt1
     * @param string $salt2
     *
     * @return string
     */
    public function passwordHash($pass, $hash = 'sha256', $salt1 = '', $salt2 = '')
    {
        return hash($hash, $pass.$salt1.$salt2);
    }

    /**
     * @param       $str
     * @param array $noStrip
     *
     * @return string
     *
     * @since version 1.6.3
     */
    public function camelCase($str, $noStrip = [])
    {
        if (is_array($str)) {
            $str = implode(' ',$str);
        }
        if (!is_string($str)) {
            return '';
        }
        // non-alpha and non-numeric characters become spaces
        $str = preg_replace('/[^a-z0-9' . implode('', $noStrip) . ']+/i', ' ', $str);
        // uppercase the first character of each word
        $str = preg_replace('/ /','',ucwords(trim($str)));
        return lcfirst($str);
    }

}

//
if (!defined('__PDO_STARTED__')) {
    define('__PDO_STARTED__',1);
    // @TODO use a language global definition
    if (!defined('LANG_ERRORS')) {
        //config error messages
        //define('LANG_ERRORS','Errors: ');
        //define('LANG_UNCAUGHT_EXCEPTION','Uncaught Exception: ');
        //CRUD
        //define('LANG_CANCEL','Cancel');
        //define('LANG_SAVE','Save');
        //define('LANG_NEW','New');
        //define('LANG_CREATE','Create');
        //define('LANG_UPDATE','Update');
        //define('LANG_DELETE','Delete');
        //define('LANG_NEXT','Next');
        //define('LANG_PREV','Previous');
        //define('LANG_FIRST','First');
        //define('LANG_LAST','Last');
        //
        //define('LANG_MONTHS','Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec');
        //define('LANG_DATE_FROM','From ');
        //define('LANG_DATE_TO',' to ');
        //define('LANG_DATE_AND',' and ');
        //define('LANG_DATE_OF',' of ');
        //config error messages
        define('LANG_ERRORS','Errores: ');
        define('LANG_UNCAUGHT_EXCEPTION','Excepci&oacute;n no controlada: ');
        //CRUD
        define('LANG_CANCEL','Cancelar');
        define('LANG_SAVE','Guardar');
        define('LANG_NEW','Nuevo');
        define('LANG_CREATE','Crear');
        define('LANG_UPDATE','Actualizar');
        define('LANG_DELETE','Borrar');
        define('LANG_NEXT','Siguiente');
        define('LANG_PREV','Anterior');
        define('LANG_FIRST','Primero');
        define('LANG_LAST','Último');
        //
        define('LANG_FULL_DAYS','Domingo,Lunes,Martes,Mi&eacute;rcoles,Jueves,Viernes,S&aacute;bado,Domingo');
        define('LANG_DAYS','Dom,Lun,Mar,Mie,Jue,Vie,Sab,Dom');
        define('LANG_FULL_MONTHS',',Enero,Febrero,Marzo,Abril,Mayo,Junio,Julio,Agosto,Septiembre,Octubre,Noviembre,Diciembre');
        define('LANG_MONTHS','Ene,Feb,Mar,Abr,May,Jun,Jul,Ago,Sep,Oct,Nov,Dic');
        define('LANG_DATE_FROM','Desde ');
        define('LANG_DATE_TO',' a ');
        define('LANG_DATE_AND',' y ');
        define('LANG_DATE_OF',' de ');
    }
//
    //require_once 'db/Config.php';
}

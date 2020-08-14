<?php
/**
 *
 * This file is part of Aura for PHP.
 *
 * @license http://opensource.org/licenses/bsd-license.php BSD
 *
 */
namespace Aura\SqlSchema;

/**
 *
 * Microsoft SQL Server schema discovery tools.
 *
 * @package Aura.SqlSchema
 *
 */
class SqlsrvSchema extends AbstractSchema
{
    /**
     *
     * The quote prefix for identifier names.
     *
     * @var string
     *
     */
    protected $quote_name_prefix = '[';

    /**
     *
     * The quote suffix for identifier names.
     *
     * @var string
     *
     */
    protected $quote_name_suffix = ']';

    /**
     *
     * Returns a list of all tables in the database.
     *
     * @param string $schema Fetch tbe list of tables in this schema;
     * when empty, uses the default schema.
     *
     * @return string[] All table names in the database.
     *
     * @todo Honor the $schema param.
     *
     */
    public function fetchTableList($schema = null)
    {
        $text = "SELECT name FROM sysobjects WHERE type = 'U' ORDER BY name";
        return $this->pdoFetchCol($text);
    }

    /**
     *
     * Returns an array of columns in a table.
     *
     * @param string $spec Return the columns in this table. This may be just
     * a `table` name, or a `schema.table` name.
     *
     * @return Column[] An associative array where the key is the column name
     * and the value is a Column object.
     *
     * @todo Honor `schema.table` as the specification.
     *
     */
    public function fetchTableCols($spec)
    {
        // no need for $schema yet
        list(,$table) = $this->splitName($spec);

        // get column info
        $text = "exec sp_columns @table_name = " . $this->quoteName($table);
        $raw_cols = $this->pdoFetchAll($text);

        // get primary key info
        $text = "exec sp_pkeys @table_owner = " . $raw_cols[0]['TABLE_OWNER']
              . ", @table_name = " . $this->quoteName($table);
        $raw_keys = $this->pdoFetchAll($text);
        $keys = array();
        foreach ($raw_keys as $row) {
            $keys[] = $row['COLUMN_NAME'];
        }

        $cols = array();
        foreach ($raw_cols as $row) {

            $name = $row['COLUMN_NAME'];

            $pos = strpos($row['TYPE_NAME'], ' ');
            if ($pos === false) {
                $type = $row['TYPE_NAME'];
            } else {
                $type = substr($row['TYPE_NAME'], 0, $pos);
            }

            // save the column description
            $cols[$name] = $this->column_factory->newInstance(
                $name,
                $type,
                $row['PRECISION'],
                $row['SCALE'],
                ! $row['NULLABLE'],
                $row['COLUMN_DEF'],
                strpos(strtolower($row['TYPE_NAME']), 'identity') !== false,
                in_array($name, $keys)
            );
        }

        return $cols;
    }

    public function fetchCurrentSchema()
    {
        $stmt = $this->pdo->query('SELECT SCHEMA_NAME()');
        return $stmt->fetchColumn();
    }

    protected function getAutoincSql()
    {
        return "COLUMNPROPERTY(
                    OBJECT_ID(COLUMNS.TABLE_SCHEMA + '.' + COLUMNS.TABLE_NAME),
                    COLUMNS.COLUMN_NAME,
                    'IsIdentity'
                )";
    }

    protected function getDefault($default)
    {
        // no default
        if ($default === null) {
            return null;
        }

        // sql server wraps non-nulls in parens
        while (
            substr($default, 0, 1) == '('
            && substr($default, -1) == ')'
        ) {
            $default = substr($default, 1, -1);
        }

        // sql null
        if (strtoupper($default) === 'NULL') {
            return null;
        }

        // numeric value
        if (is_numeric($default)) {
            return $default;
        }

        // single-quoted string
        if (
            substr($default, 0, 1) == "'"
            && substr($default, -1) == "'"
        ) {
            return substr($default, 1, -1);
        }

        // sql expression, can't do anything with it here
        return null;
    }
}

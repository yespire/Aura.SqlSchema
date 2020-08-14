<?php
/**
 *
 * This file is part of Aura for PHP.
 *
 * @license http://opensource.org/licenses/bsd-license.php BSD
 *
 */
namespace Aura\SqlSchema;

use PDO;

/**
 *
 * Abstract schema discovery tools.
 *
 * @package Aura.SqlSchema
 *
 */
abstract class AbstractSchema implements SchemaInterface
{
    /**
     *
     * The quote prefix for identifier names.
     *
     * @var string
     *
     */
    protected $quote_name_prefix = '"';

    /**
     *
     * The quote suffix for identifier names.
     *
     * @var string
     *
     */
    protected $quote_name_suffix = '"';

    /**
     *
     * A ColumnFactory for returning column information.
     *
     * @var ColumnFactory
     *
     */
    protected $column_factory;

    /**
     *
     * A Pdo connection.
     *
     * @var PDO
     *
     */
    protected $pdo;

    /**
     *
     * Constructor.
     *
     * @param PDO $pdo A database connection.
     *
     * @param ColumnFactory $column_factory A column object factory.
     *
     */
    public function __construct(PDO $pdo, ColumnFactory $column_factory)
    {
        $this->pdo = $pdo;
        $this->column_factory = $column_factory;
    }

    /**
     *
     * Returns the column factory object.
     *
     * @return ColumnFactory
     *
     */
    public function getColumnFactory()
    {
        return $this->column_factory;
    }

    /**
     *
     * Given a column specification, parse into datatype, size, and
     * decimal scale.
     *
     * @param string $spec The column specification; for example,
     * "VARCHAR(255)" or "NUMERIC(10,2)".
     *
     * @return array A sequential array of the column type, size, and scale.
     *
     */
    protected function getTypeSizeScope($spec)
    {
        $spec  = strtolower($spec);
        $size  = null;
        $scale = null;

        // find the parens, if any
        $pos = strpos($spec, '(');
        if ($pos === false) {
            // no parens, so no size or scale
            $type = $spec;
        } else {
            // find the type first.
            $type = substr($spec, 0, $pos);

            // there were parens, so there's at least a size.
            // remove parens to get the size.
            $size = trim(substr($spec, $pos), '()');

            // a comma in the size indicates a scale.
            $pos = strpos($size, ',');
            if ($pos !== false) {
                $scale = substr($size, $pos + 1);
                $size  = substr($size, 0, $pos);
            }
        }

        return array($type, $size, $scale);
    }

    /**
     *
     * Returns an array of columns in a table.
     *
     * @param string $table Return the columns in this table. This may be just
     * a `table` name, or a `schema.table` name.
     *
     * @return Column[] An associative array where the key is the column name
     * and the value is a Column object.
     *
     */
    public function fetchTableCols($table)
    {
        $pos = strpos($table, '.');
        if ($pos === false) {
            $schema = $this->fetchCurrentSchema();
        } else {
            $schema = substr($table, 0, $pos);
            $table = substr($table, $pos + 1);
        }

        $autoinc = $this->getAutoincSql();
        $extended = $this->getExtendedSql();

        $stm = "
            SELECT
                columns.column_name as _name,
                columns.data_type as _type,
                COALESCE(
                    columns.character_maximum_length,
                    columns.numeric_precision
                ) AS _size,
                columns.numeric_scale AS _scale,
                CASE
                    WHEN columns.is_nullable = 'YES' THEN 0
                    ELSE 1
                END AS _notnull,
                columns.column_default AS _default,
                {$autoinc} AS _autoinc,
                CASE
                    WHEN table_constraints.constraint_type = 'PRIMARY KEY' THEN 1
                    ELSE 0
                END AS _primary{$extended}
            FROM information_schema.columns
                LEFT JOIN information_schema.key_column_usage
                    ON columns.table_schema = key_column_usage.table_schema
                    AND columns.table_name = key_column_usage.table_name
                    AND columns.column_name = key_column_usage.column_name
                LEFT JOIN information_schema.table_constraints
                    ON key_column_usage.table_schema = table_constraints.table_schema
                    AND key_column_usage.table_name = table_constraints.table_name
                    AND key_column_usage.constraint_name = table_constraints.constraint_name
            WHERE columns.table_schema = :schema
            AND columns.table_name = :table
            ORDER BY columns.ordinal_position
        ";

        $defs = $this->pdoFetchAll($stm, ['schema' => $schema, 'table' => $table]);
        $columns = $this->extractColumns($schema, $table, $defs);

        $cols = [];

        foreach ($columns as $name => $values) {
            $cols[$name] = $this->column_factory->newInstance(
                $name,
                $values['type'],
                $values['size'],
                $values['scale'],
                $values['notnull'],
                $values['default'],
                $values['autoinc'],
                $values['primary']
            );
        }

        return $cols;
    }

    protected function extractColumns(string $schema, string $table, array $defs)
    {
        $columns = [];
        foreach ($defs as $def) {
            if (isset($columns[$def['_name']])) {
                $columns[$def['_name']]['primary'] = $columns[$def['_name']]['primary'] ?: (bool) $def['_primary'];
                continue;
            }
            $columns[$def['_name']] = $this->extractColumn($schema, $table, $def);
        }
        return $columns;
    }

    protected function extractColumn(string $schema, string $table, array $def)
    {
        return [
            'name' => $def['_name'],
            'type' => $def['_type'],
            'size' => isset($def['_size']) ? (int) $def['_size'] : null,
            'scale' => isset($def['_scale']) ? (int) $def['_scale'] : null,
            'notnull' => (bool) $def['_notnull'],
            'default' => $this->extractDefault($def['_default'], $def['_type']),
            'autoinc' => (bool) $def['_autoinc'],
            'primary' => (bool) $def['_primary'],
            'options' => null,
        ];
    }

    protected function extractDefault($default, string $type)
    {
        $type = strtolower($type);
        $default = $this->getDefault($default);

        if ($default === null) {
            return $default;
        }

        if (strpos($type, 'int') !== false) {
            return (int) $default;
        }

        if ($type == 'float' || $type == 'double' || $type == 'real') {
            return (float) $default;
        }

        return $default;
    }

    protected function getExtendedSql()
    {
        return '';
    }

    /**
     *
     * Splits an identifier name into two parts, based on the location of the
     * first dot.
     *
     * @param string $name The identifier name to be split.
     *
     * @return array An array of two elements; element 0 is the parts before
     * the dot, and element 1 is the part after the dot. If there was no dot,
     * element 0 will be null and element 1 will be the name as given.
     *
     */
    protected function splitName($name)
    {
        $pos = strpos($name, '.');
        if ($pos === false) {
            return array(null, $name);
        } else {
            return array(substr($name, 0, $pos), substr($name, $pos+1));
        }
    }

    /**
     *
     * Quotes a single identifier name (table, table alias, table column,
     * index, sequence).
     *
     * If the name contains `' AS '`, this method will separately quote the
     * parts before and after the `' AS '`.
     *
     * If the name contains a space, this method will separately quote the
     * parts before and after the space.
     *
     * If the name contains a dot, this method will separately quote the
     * parts before and after the dot.
     *
     * @param string $name The identifier name to quote.
     *
     * @return string The quoted identifier name.
     *
     * @see replaceName()
     *
     */
    public function quoteName($name)
    {
        // remove extraneous spaces
        $name = trim($name);

        // "name"."name"
        $pos = strrpos($name, '.');
        if ($pos) {
            $one = $this->quoteName(substr($name, 0, $pos));
            $two = $this->quoteName(substr($name, $pos + 1));
            return "{$one}.{$two}";
        }

        // "name"
        return $this->quote_name_prefix . $name . $this->quote_name_suffix;
    }

    /**
     *
     * Fetch all result rows.
     *
     * @param string $statement The SQL statement.
     *
     * @param array $values Values to bind to the SQL statement.
     *
     * @return array
     *
     */
    protected function pdoFetchAll($statement, array $values = array())
    {
        $sth = $this->pdo->prepare($statement);
        $sth->execute($values);
        return $sth->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     *
     * Fetch the first column of all result rows.
     *
     * @param string $statement The SQL statement.
     *
     * @param array $values Values to bind to the SQL statement.
     *
     * @return array
     *
     */
    protected function pdoFetchCol($statement, array $values = array())
    {
        $sth = $this->pdo->prepare($statement);
        $sth->execute($values);
        return $sth->fetchAll(PDO::FETCH_COLUMN, 0);
    }

    /**
     *
     * Fetch the first column of the first row.
     *
     * @param string $statement The SQL statement.
     *
     * @param array $values Values to bind to the SQL statement.
     *
     * @return mixed
     *
     */
    protected function pdoFetchValue($statement, array $values = array())
    {
        $sth = $this->pdo->prepare($statement);
        $sth->execute($values);
        return $sth->fetchColumn(0);
    }

    abstract public function fetchCurrentSchema();

    abstract protected function getAutoincSql();

    abstract protected function getDefault($default);

}

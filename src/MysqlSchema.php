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
 * MySQL schema discovery tools.
 *
 * @package Aura.SqlSchema
 *
 */
class MysqlSchema extends AbstractSchema
{
    /**
     *
     * The quote prefix for identifier names.
     *
     * @var string
     *
     */
    protected $quote_name_prefix = '`';

    /**
     *
     * The quote suffix for identifier names.
     *
     * @var string
     *
     */
    protected $quote_name_suffix = '`';

    protected $maria = false;

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
        parent::__construct($pdo, $column_factory);

        $stmt = $pdo->query("SHOW VARIABLES LIKE '%version%'");
        $vars  = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);
        if (isset($vars['version']) && stripos($vars['version'], 'maria')) {
            $this->maria = true;
        }
    }

     /**
     *
     * Returns a list of tables in the database.
     *
     * @param string $schema Optionally, pass a schema name to get the list
     * of tables in this schema.
     *
     * @return string[] The list of table-names in the database.
     *
     */
    public function fetchTableList($schema = null)
    {
        $text = 'SHOW TABLES';
        if ($schema) {
            $text .= ' IN ' . $this->quoteName($schema);
        }
        return $this->pdoFetchCol($text);
    }

    public function fetchCurrentSchema()
    {
        $stmt = $this->pdo->query('SELECT DATABASE()');
        return $stmt->fetchColumn();
    }

    protected function getAutoincSql()
    {
        return "CASE
                    WHEN LOCATE('auto_increment', columns.EXTRA) > 0 THEN 1
                    ELSE 0
                END";
    }

    protected function getExtendedSql()
    {
        return ',
                columns.column_type as _extended';
    }

    protected function extractColumn(string $schema, string $table, array $def)
    {
        $column = parent::extractColumn($schema, $table, $def);

        if (
            $this->maria
            && $column['notnull'] == 0
            && $column['default'] === 'NULL'
        ) {
            $column['default'] = null;
        }

        $extended = trim($def['_extended']);

        $pos = stripos($extended, 'unsigned');
        if ($pos !== false) {
            $column['type'] .= ' ' . substr($extended, $pos, 8);
            return $column;
        }

        $pos = stripos($extended, 'enum');
        if ($pos === 0) {
            $input = trim(substr($extended, 4), '()');
            $column['options'] = str_getcsv($input);
            return $column;
        }

        return $column;
    }


    /**
     *
     * A helper method to get the default value for a column.
     *
     * @param string $default The default value as reported by MySQL.
     *
     * @return string
     *
     */
    protected function getDefault($default)
    {
        if ($default === null) {
            return null;
        }

        if (strtoupper($default) == 'CURRENT_TIMESTAMP') {
            // the only non-literal allowed by MySQL is "CURRENT_TIMESTAMP"
            return null;
        }

        return $default;
    }

}

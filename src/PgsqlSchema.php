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
 * PostgreSQL schema discovery tools.
 *
 * @package Aura.SqlSchema
 *
 */
class PgsqlSchema extends AbstractSchema
{
    /**
     *
     * Returns a list of all tables in the database.
     *
     * @param string $schema Fetch tbe list of tables in this schema;
     * when empty, uses the default schema.
     *
     * @return string[] All table names in the database.
     *
     */
    public function fetchTableList($schema = null)
    {
        if ($schema) {
            $cmd = "
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = :schema
            ";
            $values = array('schema' => $schema);
        } else {
            $cmd = "
                SELECT table_schema || '.' || table_name
                FROM information_schema.tables
                WHERE table_schema != 'pg_catalog'
                AND table_schema != 'information_schema'
            ";
            $values = array();
        }

        return $this->pdoFetchCol($cmd, $values);
    }

    /**
     *
     * Given a native column SQL default value, finds a PHP literal value.
     *
     * SQL NULLs are converted to PHP nulls.  Non-literal values (such as
     * keywords and functions) are also returned as null.
     *
     * @param string $default The column default SQL value.
     *
     * @return scalar A literal PHP value.
     *
     */
    protected function getDefault($default)
    {
        // null?
        if ($default === null || strtoupper($default) === 'NULL') {
            return null;
        }

        // numeric literal?
        if (is_numeric($default)) {
            return $default;
        }

        // string literal?
        $k = substr($default, 0, 1);
        if ($k == '"' || $k == "'") {
            // find the trailing :: typedef
            $pos = strrpos($default, '::');
            // also remove the leading and trailing quotes
            return substr($default, 1, $pos-2);
        }

        return null;
    }

    public function fetchCurrentSchema()
    {
        $stmt = $this->pdo->query('SELECT CURRENT_SCHEMA');
        return $stmt->fetchColumn();
    }

    protected function getAutoincSql()
    {
        return "CASE
                    WHEN SUBSTRING(columns.COLUMN_DEFAULT FROM 1 FOR 7) = 'nextval' THEN 1
                    ELSE 0
                END";
    }


}

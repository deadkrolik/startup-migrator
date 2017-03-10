package migrator

import (
	"database/sql"
	//custom engine
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

//EngineMysql - engine to execute queries in MySQL
type EngineMysql struct {
	dsn        string
	connection *sql.DB
	table      string
}

//GetEngineMysql - get engine object with connection config like "user:pass@/dbname?charset=utf8"
func GetEngineMysql(dsn string) *EngineMysql {
	return &EngineMysql{dsn: dsn}
}

const (
	migrationsMysql = "CREATE TABLE {TABLE} (" +
		"`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY," +
		"`hash` varchar(32) NOT NULL," +
		"`statement` varchar(600) NOT NULL," +
		"`created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
)

//PrepareConnection - init connection and migrations table
func (engine *EngineMysql) PrepareConnection(tableName string) error {
	engine.table = tableName

	conn, err := sql.Open("mysql", engine.dsn)
	if err != nil {
		return err
	}
	engine.connection = conn

	isExists, err := engine.isMigrationsTableExists()
	if err != nil {
		return err
	}

	if !isExists {
		err := engine.ExecQuery(strings.Replace(migrationsMysql, "{TABLE}", engine.table, 1))
		if err != nil {
			return err
		}
	}

	return nil
}

//ExecQuery - execute one query
func (engine *EngineMysql) ExecQuery(query string) error {
	_, err := engine.connection.Exec(query)

	return err
}

//IsMigrationApplied - is migration already executed (pass query md5-hash as parameter)
func (engine *EngineMysql) IsMigrationApplied(hash string) (bool, error) {
	var count int

	err := engine.connection.QueryRow(
		"SELECT COUNT(*) AS m_count FROM "+engine.table+" WHERE hash = ?",
		hash,
	).Scan(&count)

	if err != nil {
		return false, err
	}

	return count > 0, nil
}

//RegisterMigration - add new migration record to table
func (engine *EngineMysql) RegisterMigration(hash, query string) error {
	prepared, err := engine.connection.Prepare(
		"INSERT INTO " + engine.table + " SET hash = ?, statement = ?",
	)
	if err != nil {
		return err
	}

	_, err = prepared.Exec(hash, query)
	return err
}

//Disconnect - do a cleanup
func (engine *EngineMysql) Disconnect() {
	_ = engine.connection.Close()
}

//isMigrationsTableExists - if migrations table exists
func (engine *EngineMysql) isMigrationsTableExists() (bool, error) {
	var exist string

	err := engine.connection.QueryRow("SHOW TABLES LIKE '" + engine.table + "'").Scan(&exist)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return exist == engine.table, nil
}

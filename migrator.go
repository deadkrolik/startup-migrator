package migrator

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	//custom engine
	_ "github.com/go-sql-driver/mysql"
)

//StartUpMigrator - migrations, built-in the app
type StartUpMigrator struct {
	connectionConfig string
	connection       *sql.DB
}

//StatementResult - migration query result
type StatementResult struct {
	IsSuccess    bool
	Statement    string
	Hash         string
	ErrorMessage string
}

const (
	migrationsTableName   = "strtp_mmigrator"
	migrationsTableCreate = "CREATE TABLE strtp_mmigrator (" +
		"`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY," +
		"`hash` varchar(32) NOT NULL," +
		"`statement` varchar(600) NOT NULL," +
		"`created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
)

//GetMigrator - get new migrator object with connect string "user:pass@/DBNAME?charset=utf8"
func GetMigrator(config string) (*StartUpMigrator, error) {
	migrator := StartUpMigrator{
		connectionConfig: config,
	}

	err := migrator.prepare()
	if err != nil {
		return nil, err
	}

	return &migrator, nil
}

//Run - run queries execution
func (migrator *StartUpMigrator) Run(statements []string) []StatementResult {

	var result []StatementResult
	defer migrator.connection.Close()

	for _, statement := range statements {

		//if it's already applied
		hash := fmt.Sprintf("%x", md5.Sum([]byte(statement)))

		isApplied, err := migrator.isMigrationApplied(hash)
		if err != nil {
			return migrator.returnErrorStatement(result, statement, err)
		}

		if isApplied {
			continue
		}

		//exec query
		_, err = migrator.connection.Exec(statement)

		//something went wrong
		if err != nil {
			return migrator.returnErrorStatement(result, statement, err)
		}

		//register new migration
		prepared, err := migrator.connection.Prepare(
			"INSERT INTO " + migrationsTableName + " SET hash = ?, statement = ?",
		)
		if err != nil {
			return migrator.returnErrorStatement(result, statement, err)
		}
		_, err = prepared.Exec(hash, statement)
		if err != nil {
			return migrator.returnErrorStatement(result, statement, err)
		}

		result = append(result, StatementResult{
			IsSuccess: true,
			Statement: statement,
			Hash:      hash,
		})
	}

	return result
}

//returnErrorStatement - add error query to result
func (migrator *StartUpMigrator) returnErrorStatement(result []StatementResult, query string, err error) []StatementResult {

	result = append(result, StatementResult{
		IsSuccess:    false,
		Statement:    query,
		Hash:         migrator.getHash(query),
		ErrorMessage: err.Error(),
	})
	return result
}

//getHash - hash of query string
func (migrator *StartUpMigrator) getHash(query string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(query)))
}

//prepare - init connection and migrations table
func (migrator *StartUpMigrator) prepare() error {

	conn, err := sql.Open("mysql", migrator.connectionConfig)
	if err != nil {
		return err
	}
	migrator.connection = conn

	isExists, err := migrator.isMigrationsTableExists()
	if err != nil {
		return err
	}

	if !isExists {
		_, err := migrator.connection.Exec(migrationsTableCreate)
		if err != nil {
			return err
		}
	}

	return nil
}

//isMigrationApplied - is migration already executed
func (migrator *StartUpMigrator) isMigrationApplied(hash string) (bool, error) {

	var count uint64

	err := migrator.connection.QueryRow(
		"SELECT COUNT(*) AS m_count FROM "+migrationsTableName+" WHERE hash = ?", hash,
	).Scan(&count)

	if err != nil {
		return false, err
	}

	return count > 0, nil
}

//isMigrationsTableExists - if migrations table exists
func (migrator *StartUpMigrator) isMigrationsTableExists() (bool, error) {

	var exist string

	err := migrator.connection.QueryRow("SHOW TABLES LIKE '" + migrationsTableName + "'").Scan(&exist)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return exist == migrationsTableName, nil
}

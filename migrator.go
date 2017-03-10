package migrator

import (
	"crypto/md5"
	"fmt"
)

//StartUpMigrator - migrations, built-in the app
type StartUpMigrator struct {
	engine Engine
}

//Engine - specific database driver
type Engine interface {
	PrepareConnection(dsn string) error
	ExecQuery(query string) error
	IsMigrationApplied(hash string) (bool, error)
	RegisterMigration(hash, query string) error
	Disconnect()
}

//StatementResult - migration query result
type StatementResult struct {
	IsSuccess    bool
	Statement    string
	Hash         string
	ErrorMessage string
}

//GetMigrator - get new migrator object
func GetMigrator(tableName string, engine Engine) (*StartUpMigrator, error) {
	migrator := StartUpMigrator{engine: engine}

	err := migrator.engine.PrepareConnection(tableName)
	if err != nil {
		return nil, err
	}

	return &migrator, nil
}

//Run - start migrations execution. Returns executed statements and general error
func (migrator *StartUpMigrator) Run(statements []string) ([]StatementResult, error) {

	var result []StatementResult
	defer migrator.engine.Disconnect()

	for _, statement := range statements {

		//if it's already applied
		hash := migrator.getHash(statement)
		isApplied, err := migrator.engine.IsMigrationApplied(hash)
		if err != nil {
			return migrator.getErrorStatement(result, statement, err)
		}

		if isApplied {
			continue
		}

		//something went wrong
		err = migrator.engine.ExecQuery(statement)
		if err != nil {
			return migrator.getErrorStatement(result, statement, err)
		}

		//register new migration
		err = migrator.engine.RegisterMigration(hash, statement)
		if err != nil {
			return migrator.getErrorStatement(result, statement, err)
		}

		result = append(result, StatementResult{
			IsSuccess: true,
			Statement: statement,
			Hash:      hash,
		})
	}

	return result, nil
}

//returnErrorStatement - add error query to result
func (migrator *StartUpMigrator) getErrorStatement(result []StatementResult, query string, err error) ([]StatementResult, error) {
	result = append(result, StatementResult{
		IsSuccess:    false,
		Statement:    query,
		Hash:         migrator.getHash(query),
		ErrorMessage: err.Error(),
	})

	return result, err
}

//getHash - hash of query string
func (migrator *StartUpMigrator) getHash(query string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(query)))
}

// Package migrator provides execution of sql-queries once at application start
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

//GetMigrator - get new migrator object
func GetMigrator(tableName string, engine Engine) (*StartUpMigrator, error) {
	migrator := StartUpMigrator{engine: engine}

	err := migrator.engine.PrepareConnection(tableName)
	if err != nil {
		return nil, err
	}

	return &migrator, nil
}

//Run - start migrations execution
func (migrator *StartUpMigrator) Run(statements []string) error {
	defer migrator.engine.Disconnect()

	for _, statement := range statements {

		// Check if the migration already applied
		hash := migrator.getHash(statement)
		isApplied, err := migrator.engine.IsMigrationApplied(hash)
		if err != nil {
			return err
		}

		if isApplied {
			continue
		}

		err = migrator.engine.ExecQuery(statement)
		if err != nil {
			return err
		}

		// Register new migration in migrations table
		err = migrator.engine.RegisterMigration(hash, statement)
		if err != nil {
			return err
		}
	}

	return nil
}

//getHash - hash of sql-query string
func (migrator *StartUpMigrator) getHash(query string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(query)))
}

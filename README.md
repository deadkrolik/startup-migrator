# Non deployment migration package

## Introduction

> When you write small golang application and don't need to have any deployment procedure you may also need to have specific database structure before start your application. This package is created to fit this need. You just describe sql-statements in the code and run execution only once - before any custom application work.

## Code Samples

Here is a simple code:

```go
package main

import (
	startupMigrator "github.com/deadkrolik/startup-migrator"
)

func loadMigrations() {
	dsn := "root:root@/dcheck?charset=utf8"
	migrator, err := startupMigrator.GetMigrator("migrations", startupMigrator.GetEngineMysql(dsn))
	if err != nil {
		panic(err)
	}
	_, err = migrator.Run([]string{
		"CREATE TABLE aaa (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY)",
		"CREATE TABLE aaa2 (id2 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY)",
	})
	if err != nil {
		panic(err)
	}
}
```

## Installation

Run this command
> go get -u github.com/deadkrolik/startup-migrator

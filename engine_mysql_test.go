package migrator

import (
	"database/sql"
	"os"
	//
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"testing"
)

var (
	user string
	pass string
	db   string
)

func init() {
	user = os.Getenv("TEST_USER")
	pass = os.Getenv("TEST_PASS")

	if user == "" || pass == "" {
		panic("Please run tests using this command: TEST_USER=... TEST_PASS=... go test")
	}

	db = "migrator_test_db"
}

func TestMigrationTableCreated(t *testing.T) {
	conn := setUp()
	defer tearDown(conn)

	err := GetEngineMysql(user + ":" + pass + "@/" + db + "?charset=utf8").PrepareConnection("migrations")
	if err != nil {
		t.Error("Can't execute PrepareConnection", err)
	}
}

func setUp() *sql.DB {
	conn, err := sql.Open("mysql", user+":"+pass+"@/?charset=utf8")
	if err != nil {
		panic(err)
	}

	_, err = conn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db))
	if err != nil {
		panic(err)
	}

	_, err = conn.Exec(fmt.Sprintf("USE %s", db))
	if err != nil {
		panic(err)
	}

	return conn
}

func tearDown(conn *sql.DB) {
	var err error

	_, err = conn.Exec(fmt.Sprintf("DROP DATABASE %s", db))
	if err != nil {
		_ = conn.Close()
		panic(err)
	}

	_ = conn.Close()
}

package main

import (
	"database/sql"
	"flag"
	"log"
	"time"

	sqlite "github.com/mattn/go-sqlite3"
)

var sourceFile = flag.String("source", "source.db", "Source file for backup")
var destFile = flag.String("dest", "dest.db", "Dest file for backup")
var driverName = "sqlite3_backup"
var timeout int64 = 10

func main() {
	flag.Parse()

	driverConns := []*sqlite.SQLiteConn{}
	sql.Register(driverName, &sqlite.SQLiteDriver{
		ConnectHook: func(conn *sqlite.SQLiteConn) error {
			driverConns = append(driverConns, conn)
			return nil
		},
	})

	srcDb, err := sql.Open(driverName, *sourceFile)
	if err != nil {
		log.Fatal(err)
	}

	defer srcDb.Close()
	srcDb.Ping()

	dstDb, err := sql.Open(driverName, *destFile)
	if err != nil {
		log.Fatal(err)
	}
	defer dstDb.Close()
	dstDb.Ping()

	// Check the driver connections.
	if len(driverConns) != 2 {
		log.Fatalf("Expected 2 driver connections, but found %v.", len(driverConns))
	}
	srcDbDriverConn := driverConns[0]
	if srcDbDriverConn == nil {
		log.Fatal("The source database driver connection is nil.")
	}
	destDbDriverConn := driverConns[1]
	if destDbDriverConn == nil {
		log.Fatal("The destination database driver connection is nil.")
	}

	backup, err := destDbDriverConn.Backup("main", srcDbDriverConn, "main")
	if err != nil {
		log.Fatal("Error calling backup", err)
	}

	isDone, err := backup.Step(0)
	if err != nil {
		log.Fatal("Unable to perform an initial 0-page backup step:", err)
	}
	if isDone {
		log.Fatal("Backup is unexpectedly done.")
	}

	// Check that the page count and remaining values are reasonable.
	initialPageCount := backup.PageCount()
	if initialPageCount <= 0 {
		log.Fatalf("Unexpected initial page count value: %v", initialPageCount)
	}
	initialRemaining := backup.Remaining()
	if initialRemaining <= 0 {
		log.Fatalf("Unexpected initial remaining value: %v", initialRemaining)
	}
	if initialRemaining != initialPageCount {
		log.Fatalf("Initial remaining value differs from the initial page count value; remaining: %v; page count: %v", initialRemaining, initialPageCount)
	}

	var startTime = time.Now().Unix()

	// Test backing-up using a page-by-page approach.
	var latestRemaining = initialRemaining
	for {
		// Perform the backup step.
		isDone, err = backup.Step(1)
		if err != nil {
			log.Fatal("Failed to perform a backup step:", err)
		}

		// The page count should remain unchanged from its initial value.
		currentPageCount := backup.PageCount()
		if currentPageCount != initialPageCount {
			log.Fatalf("Current page count differs from the initial page count; initial page count: %v; current page count: %v", initialPageCount, currentPageCount)
		}

		// There should now be one less page remaining.
		currentRemaining := backup.Remaining()
		expectedRemaining := latestRemaining - 1
		if currentRemaining != expectedRemaining {
			log.Fatalf("Unexpected remaining value; expected remaining value: %v; actual remaining value: %v", expectedRemaining, currentRemaining)
		}
		latestRemaining = currentRemaining

		if isDone {
			break
		}

		// Limit the runtime of the backup attempt.
		if (time.Now().Unix() - startTime) > timeout {
			log.Fatal("Backup is taking longer than expected.")
		}
	}

	// Check that the page count and remaining values are reasonable.
	finalPageCount := backup.PageCount()
	if finalPageCount != initialPageCount {
		log.Fatalf("Final page count differs from the initial page count; initial page count: %v; final page count: %v", initialPageCount, finalPageCount)
	}
	finalRemaining := backup.Remaining()
	if finalRemaining != 0 {
		log.Fatalf("Unexpected remaining value: %v", finalRemaining)
	}

	// Finish the backup.
	err = backup.Finish()
	if err != nil {
		log.Fatal("Failed to finish backup:", err)
	}

}

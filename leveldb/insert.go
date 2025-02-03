package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	// MySQL database connection details
	sourceDSN := "root:Sabbir@123@tcp(127.0.0.1:3306)/acc"
	sourceDB, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		log.Fatal("Failed to connect to MySQL database:", err)
	}
	defer sourceDB.Close()

	// LevelDB connection details
	levelDB, err := leveldb.OpenFile("db", nil)
	if err != nil {
		log.Fatal("Failed to open LevelDB:", err)
	}
	defer levelDB.Close()

	// Query to read data from the lineitem table
	query := `SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, 
	                 L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, 
	                 L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, 
	                 L_SHIPMODE, L_COMMENT FROM lineitem limit 6000000`

	rows, err := sourceDB.Query(query)
	if err != nil {
		log.Fatal("Failed to execute query:", err)
	}
	defer rows.Close()

	// Measure total time for the operation
	startTime := time.Now()
	ct := 1
	for rows.Next() {
		var (
			L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER                    int
			L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX                    float64
			L_RETURNFLAG, L_LINESTATUS, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT string
			L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE                           string
		)

		err := rows.Scan(
			&L_ORDERKEY, &L_PARTKEY, &L_SUPPKEY, &L_LINENUMBER, &L_QUANTITY,
			&L_EXTENDEDPRICE, &L_DISCOUNT, &L_TAX, &L_RETURNFLAG, &L_LINESTATUS,
			&L_SHIPDATE, &L_COMMITDATE, &L_RECEIPTDATE, &L_SHIPINSTRUCT,
			&L_SHIPMODE, &L_COMMENT,
		)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		// Create key and value pairs
		key := fmt.Sprintf("%d_%d_%d_%d", L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
		value := fmt.Sprintf("%d|%d|%d|%d|%f|%f|%f|%f|%s|%s|%s|%s|%s|%s|%s|%s",
			L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER,
			L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX,
			L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE,
			L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT)

		err = levelDB.Put([]byte(key), []byte(value), nil)
		if err != nil {
			log.Fatal("Failed to insert into LevelDB:", err)
		}

		totalDuration := time.Since(startTime)
		ct += 1
		if ct%10000 == 0 {
			fmt.Printf("Inserting : %v, Time: %s \n", ct, totalDuration)
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal("Error encountered during rows iteration:", err)
	}

	// Calculate total elapsed time
	totalDuration := time.Since(startTime)
	fmt.Printf("Total data transfer time: %s\n", totalDuration)
}

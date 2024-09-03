package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Database connection details
	dsn := "root:Sabbir@123@tcp(127.0.0.1:3306)/tpch"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// Open the lineitem.tbl file
	file, err := os.Open("/home/parallels/Desktop/tpch/lineitem.tbl")
	if err != nil {
		log.Fatal("Failed to open file:", err)
	}
	defer file.Close()

	// Prepare the SQL insert statement
	insertStmt := `INSERT INTO lineitem (
		L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, 
		L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, 
		L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, 
		L_SHIPMODE, L_COMMENT
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	// Prepare the insert statement once to optimize performance
	stmt, err := db.Prepare(insertStmt)
	if err != nil {
		log.Fatal("Failed to prepare insert statement:", err)
	}
	defer stmt.Close()

	// Read and parse the lineitem.tbl file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "|")

		// Exclude the last element as it's an empty string due to the trailing "|"
		if len(fields) < 16 {
			log.Fatal("Unexpected data format in line:", line)
		}

		// Execute the insert statement
		_, err := stmt.Exec(
			fields[0], fields[1], fields[2], fields[3], fields[4],
			fields[5], fields[6], fields[7], fields[8], fields[9],
			fields[10], fields[11], fields[12], fields[13], fields[14], fields[15],
		)
		if err != nil {
			log.Fatal("Failed to insert record:", err)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal("Failed to read file:", err)
	}

	fmt.Println("Data inserted successfully.")
}

CREATE TABLE lineitem6m_enc (
    L_ORDERKEY VARCHAR(256),
    L_PARTKEY VARCHAR(256),
    L_SUPPKEY VARCHAR(256),
    L_LINENUMBER VARCHAR(256),
    L_QUANTITY VARCHAR(256),
    L_EXTENDEDPRICE VARCHAR(256),
    L_DISCOUNT VARCHAR(256),
    L_TAX VARCHAR(256),
    L_RETURNFLAG VARCHAR(256),
    L_LINESTATUS VARCHAR(256),
    L_SHIPDATE VARCHAR(256),
    L_COMMITDATE VARCHAR(256),
    L_RECEIPTDATE VARCHAR(256),
    L_SHIPINSTRUCT VARCHAR(256),
    L_SHIPMODE VARCHAR(256),
    L_COMMENT VARCHAR(256)
);

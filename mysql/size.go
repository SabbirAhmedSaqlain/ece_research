//calculate mysql database size

package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Database connection details
	dsn := "root:Sabbir@123@tcp(127.0.0.1:3306)/acc"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// Query to calculate the storage size of the lineitem table
	query := `
	SELECT 
	  table_name AS TableName, 
	  round(((data_length + index_length) / 1024 / 1024), 2) AS SizeInMB
	FROM 
	  information_schema.tables 
	WHERE 
	  table_schema = ? 
	  AND table_name = ?;
	`

	// Execute the query
	var tableName string
	var sizeInMB float64
	err = db.QueryRow(query, "acc", "lineitem6m_enc_Twofish").Scan(&tableName, &sizeInMB)
	if err != nil {
		log.Fatal("Failed to execute query:", err)
	}

	// Output the result
	fmt.Printf("Table: %s\n", tableName)
	fmt.Printf("Size: %.2f MB\n", sizeInMB)
}

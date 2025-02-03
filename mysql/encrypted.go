package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Function to encrypt a string using AES encryption
func encrypt(plainText string, key []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	// Make the cipher text a bit larger than the original text
	cipherText := make([]byte, aes.BlockSize+len(plainText))

	// Generate a random IV (Initialization Vector)
	iv := cipherText[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}

	// Encrypt the data
	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(cipherText[aes.BlockSize:], []byte(plainText))

	// Return the base64 encoded string of the encrypted text
	return base64.URLEncoding.EncodeToString(cipherText), nil
}

func main() {
	// MySQL database connection details
	sourceDSN := "root:Sabbir@123@tcp(127.0.0.1:3306)/acc"
	sourceDB, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		log.Fatal("Failed to connect to source database:", err)
	}
	defer sourceDB.Close()

	// Destination MySQL database connection details
	destDSN := "root:Sabbir@123@tcp(127.0.0.1:3306)/acc"
	destDB, err := sql.Open("mysql", destDSN)
	if err != nil {
		log.Fatal("Failed to connect to destination database:", err)
	}
	defer destDB.Close()

	// Encryption key (must be 16, 24, or 32 bytes long for AES-128, AES-192, or AES-256)
	encryptionKey := []byte("this_is_a_32_byte_encryption_key")

	// Query to read data from the source lineitem table
	query := `SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, 
	                 L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, 
	                 L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, 
	                 L_SHIPMODE, L_COMMENT FROM lineitem limit 6000000`

	rows, err := sourceDB.Query(query)
	if err != nil {
		log.Fatal("Failed to execute query:", err)
	}
	defer rows.Close()

	// Prepare insert statement for destination MySQL
	insertStmt := `INSERT INTO lineitem6m_enc (
		L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, 
		L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, 
		L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, 
		L_SHIPMODE, L_COMMENT
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	stmt, err := destDB.Prepare(insertStmt)
	if err != nil {
		log.Fatal("Failed to prepare insert statement:", err)
	}
	defer stmt.Close()

	// Measure total time for the operation
	startTime := time.Now()
	ct := 0
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

		// Encrypt each column except L_ORDERKEY
		encryptedL_PARTKEY, _ := encrypt(fmt.Sprintf("%d", L_PARTKEY), encryptionKey)
		encryptedL_SUPPKEY, _ := encrypt(fmt.Sprintf("%d", L_SUPPKEY), encryptionKey)
		encryptedL_LINENUMBER, _ := encrypt(fmt.Sprintf("%d", L_LINENUMBER), encryptionKey)
		encryptedL_QUANTITY, _ := encrypt(fmt.Sprintf("%f", L_QUANTITY), encryptionKey)
		encryptedL_EXTENDEDPRICE, _ := encrypt(fmt.Sprintf("%f", L_EXTENDEDPRICE), encryptionKey)
		encryptedL_DISCOUNT, _ := encrypt(fmt.Sprintf("%f", L_DISCOUNT), encryptionKey)
		encryptedL_TAX, _ := encrypt(fmt.Sprintf("%f", L_TAX), encryptionKey)
		encryptedL_RETURNFLAG, _ := encrypt(L_RETURNFLAG, encryptionKey)
		encryptedL_LINESTATUS, _ := encrypt(L_LINESTATUS, encryptionKey)
		encryptedL_SHIPDATE, _ := encrypt(L_SHIPDATE, encryptionKey)
		encryptedL_COMMITDATE, _ := encrypt(L_COMMITDATE, encryptionKey)
		encryptedL_RECEIPTDATE, _ := encrypt(L_RECEIPTDATE, encryptionKey)
		encryptedL_SHIPINSTRUCT, _ := encrypt(L_SHIPINSTRUCT, encryptionKey)
		encryptedL_SHIPMODE, _ := encrypt(L_SHIPMODE, encryptionKey)
		encryptedL_COMMENT, _ := encrypt(L_COMMENT, encryptionKey)

		// Measure time for each insert
		//startInsert := time.Now()
		_, err = stmt.Exec(
			L_ORDERKEY, encryptedL_PARTKEY, encryptedL_SUPPKEY, encryptedL_LINENUMBER, encryptedL_QUANTITY,
			encryptedL_EXTENDEDPRICE, encryptedL_DISCOUNT, encryptedL_TAX, encryptedL_RETURNFLAG, encryptedL_LINESTATUS,
			encryptedL_SHIPDATE, encryptedL_COMMITDATE, encryptedL_RECEIPTDATE, encryptedL_SHIPINSTRUCT,
			encryptedL_SHIPMODE, encryptedL_COMMENT,
		)
		if err != nil {
			log.Fatal("Failed to insert into destination database:", err)
		}
		// insertDuration := time.Since(startInsert)
		// fmt.Printf("Insert time: %s\n", insertDuration)

		ct += 1
		if ct%10000 == 0 {
			fmt.Printf("Inserting : %v \n", ct)
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal("Error encountered during rows iteration:", err)
	}

	// Calculate total elapsed time
	totalDuration := time.Since(startTime)
	fmt.Printf("Total data transfer time: %s\n", totalDuration)
}

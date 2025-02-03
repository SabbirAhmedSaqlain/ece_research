package main

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/crypto/chacha20"
)

var nonce = []byte("123456789012") // 12-byte nonce

// Function to encrypt a string using ChaCha20
func encryptChaCha20(plainText string, key []byte) (string, error) {
	cipher, err := chacha20.NewUnauthenticatedCipher(key, nonce)
	if err != nil {
		return "", err
	}

	plainTextBytes := []byte(plainText)
	cipherText := make([]byte, len(plainTextBytes))
	cipher.XORKeyStream(cipherText, plainTextBytes)

	// Return the base64 encoded string of the encrypted text
	return base64.URLEncoding.EncodeToString(cipherText), nil
}

func main() {
	// MySQL database connection details
	sourceDSN := "root:Sabbir@123@tcp(127.0.0.1:3306)/acc"
	sourceDB, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		log.Fatal("Failed to connect to MySQL database:", err)
	}
	defer sourceDB.Close()

	// LevelDB connection details
	levelDB, err := leveldb.OpenFile("enc_database6mcha", nil)
	if err != nil {
		log.Fatal("Failed to open LevelDB:", err)
	}
	defer levelDB.Close()

	// FIXED: Use a proper 32-byte encryption key
	encryptionKey := []byte("this_is_a_32_byte_encryption_key") // EXACTLY 32 bytes

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

		// Concatenating multiple fields to encrypt
		value := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s",
			L_RECEIPTDATE, L_SHIPMODE, L_COMMENT, L_SHIPINSTRUCT,
			L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE)

		encryptedValue, _ := encryptChaCha20(value, encryptionKey)

		// Creating a composite key for LevelDB
		key := fmt.Sprintf("%d_%d_%d_%d", L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)

		// Store encrypted data in LevelDB
		err = levelDB.Put([]byte(key), []byte(encryptedValue), nil)
		if err != nil {
			log.Fatal("Failed to insert into LevelDB:", err)
		}

		ct += 1
		if ct%10000 == 0 {
			fmt.Printf("Inserted into LevelDB: %v records\n", ct)
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal("Error encountered during rows iteration:", err)
	}

	// Calculate total elapsed time
	totalDuration := time.Since(startTime)
	fmt.Printf("Total data transfer time: %s\n", totalDuration)
}

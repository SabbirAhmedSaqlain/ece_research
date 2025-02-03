package main

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/crypto/twofish"
)

// Function to pad plaintext to be a multiple of Twofish block size (16 bytes)
func pad(src []byte, blockSize int) []byte {
	padding := blockSize - len(src)%blockSize
	for i := 0; i < padding; i++ {
		src = append(src, byte(padding))
	}
	return src
}

// Function to encrypt a string using Twofish
func encryptTwofish(plainText string, key []byte) (string, error) {
	block, err := twofish.NewCipher(key)
	if err != nil {
		return "", err
	}

	blockSize := block.BlockSize()
	paddedPlainText := pad([]byte(plainText), blockSize)

	cipherText := make([]byte, len(paddedPlainText))
	for i := 0; i < len(paddedPlainText); i += blockSize {
		block.Encrypt(cipherText[i:], paddedPlainText[i:])
	}

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
	levelDB, err := leveldb.OpenFile("enc_database6mtwofish", nil)
	if err != nil {
		log.Fatal("Failed to open LevelDB:", err)
	}
	defer levelDB.Close()

	// FIXED: Proper 32-byte encryption key
	encryptionKey := []byte("this_is_a_very_secure_32byte_key") // EXACTLY 32 bytes

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

		// Encrypt concatenated values
		value := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s",
			L_RECEIPTDATE, L_SHIPMODE, L_COMMENT, L_SHIPINSTRUCT,
			L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE)

		encryptedValue, _ := encryptTwofish(value, encryptionKey)

		// Store encrypted data in LevelDB
		key := fmt.Sprintf("%d_%d_%d_%d", L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
		err = levelDB.Put([]byte(key), []byte(encryptedValue), nil)
		if err != nil {
			log.Fatal("Failed to insert into LevelDB:", err)
		}

		ct++
		if ct%10000 == 0 {
			fmt.Printf("Inserted into LevelDB: %v records\n", ct)
		}
	}

	totalDuration := time.Since(startTime)
	fmt.Printf("Total data transfer time: %s\n", totalDuration)
}

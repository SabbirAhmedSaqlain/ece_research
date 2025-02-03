package main

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/crypto/blowfish"
)

// Function to pad plaintext to a multiple of Blowfish block size (8 bytes)
func pad(src []byte, blockSize int) []byte {
	padding := blockSize - len(src)%blockSize
	for i := 0; i < padding; i++ {
		src = append(src, byte(padding))
	}
	return src
}

// Function to encrypt a string using Blowfish
func encryptBlowfish(plainText string, key []byte) (string, error) {
	block, err := blowfish.NewCipher(key)
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
	levelDB, err := leveldb.OpenFile("enc_database1m", nil)
	if err != nil {
		log.Fatal("Failed to open LevelDB:", err)
	}
	defer levelDB.Close()

	// Encryption key (must be between 4 to 56 bytes for Blowfish)
	encryptionKey := []byte("this_is_a_secure_blowfish_key") // 32-byte key for Blowfish

	// Query to read data from the lineitem table
	query := `SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, 
	                 L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, 
	                 L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, 
	                 L_SHIPMODE, L_COMMENT FROM lineitem limit 1000000`

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

		// Encrypt only selected values using Blowfish
		key := fmt.Sprintf("%d_%d_%d_%d", L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)

		// Combine values into a single string before encryption
		value1 := L_RECEIPTDATE + L_SHIPMODE + L_COMMENT + L_SHIPINSTRUCT + L_RETURNFLAG + L_LINESTATUS + L_SHIPDATE + L_COMMITDATE
		encryptedValue, _ := encryptBlowfish(value1, encryptionKey)

		// Insert encrypted data into LevelDB
		err = levelDB.Put([]byte(key), []byte(encryptedValue), nil)
		if err != nil {
			log.Fatal("Failed to insert into LevelDB:", err)
		}

		ct += 1
		if ct%10000 == 0 {
			fmt.Printf("Inserted : %v rows\n", ct)
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal("Error encountered during rows iteration:", err)
	}

	// Calculate total elapsed time
	totalDuration := time.Since(startTime)
	fmt.Printf("Total data transfer time: %s\n", totalDuration)

	// --------------------Print LevelDB test data (Optional)--------------
	// ct = 0
	// iter := levelDB.NewIterator(nil, nil)
	// for iter.Next() {
	// 	key := iter.Key()
	// 	value := iter.Value()
	// 	ct += 1
	// 	fmt.Printf("\nKey: %s, Value: %s\n", key, value)
	// 	if ct == 5 {
	// 		break
	// 	}
	// }
}

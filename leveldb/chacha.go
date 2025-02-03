package main

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	"crypto/rand"

	_ "github.com/go-sql-driver/mysql"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/crypto/chacha20"
)

var nonce = []byte("123456789012")

// Generate a random 12-byte nonce for ChaCha20
func generateNonce() []byte {
	//nonce := make([]byte, 12) // ChaCha20 requires a 96-bit (12-byte) nonce
	_, err := rand.Read(nonce)
	if err != nil {
		log.Fatal("Error generating nonce:", err)
	}
	return nonce
}

// Function to encrypt a string using ChaCha20
func encryptChaCha20(plainText string, key []byte) (string, error) {
	//nonce := generateNonce() // Generate a random nonce

	// Create a ChaCha20 cipher
	cipher, err := chacha20.NewUnauthenticatedCipher(key, nonce)
	if err != nil {
		return "", err
	}

	plainTextBytes := []byte(plainText)
	cipherText := make([]byte, len(plainTextBytes))
	cipher.XORKeyStream(cipherText, plainTextBytes)

	// Store the nonce with the ciphertext (needed for decryption)
	fullCipherText := append(nonce, cipherText...)

	// Return the base64 encoded string of the encrypted text
	return base64.URLEncoding.EncodeToString(fullCipherText), nil
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
	levelDB, err := leveldb.OpenFile("enc_database1mcha", nil)
	if err != nil {
		log.Fatal("Failed to open LevelDB:", err)
	}
	defer levelDB.Close()

	// Encryption key (must be 32 bytes long for ChaCha20)
	encryptionKey := []byte("this_is_a_32_byte_encryption_key!!!!") // Ensure it's exactly 32 bytes

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

		// Concatenating multiple fields to encrypt
		value1 := L_RECEIPTDATE + L_SHIPMODE + L_COMMENT + L_SHIPINSTRUCT + L_RETURNFLAG + L_LINESTATUS + L_SHIPDATE + L_COMMITDATE
		encryptedValue, _ := encryptChaCha20(value1, encryptionKey)

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

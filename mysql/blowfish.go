package main

import (
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/blowfish"
)

// Pad plaintext to be a multiple of Blowfish block size (8 bytes)
func pad(src []byte, blockSize int) []byte {
	padding := blockSize - len(src)%blockSize
	for i := 0; i < padding; i++ {
		src = append(src, byte(padding))
	}
	return src
}

// Unpad removes padding after decryption
func unpad(src []byte) []byte {
	length := len(src)
	padding := int(src[length-1])
	return src[:length-padding]
}

// Encrypt plaintext using Blowfish in CBC mode
func encryptBlowfish(plainText string, key []byte) (string, error) {
	block, err := blowfish.NewCipher(key)
	if err != nil {
		return "", err
	}

	// Blowfish block size is 8 bytes
	blockSize := block.BlockSize()
	plaintextBytes := pad([]byte(plainText), blockSize)

	// Generate a random IV
	iv := make([]byte, blockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}

	mode := cipher.NewCBCEncrypter(block, iv)
	ciphertext := make([]byte, len(plaintextBytes))
	mode.CryptBlocks(ciphertext, plaintextBytes)

	// Combine IV and ciphertext, then encode as base64
	encryptedData := append(iv, ciphertext...)
	return base64.StdEncoding.EncodeToString(encryptedData), nil
}

// Decrypt ciphertext using Blowfish in CBC mode
func decryptBlowfish(cipherText string, key []byte) (string, error) {
	block, err := blowfish.NewCipher(key)
	if err != nil {
		return "", err
	}

	cipherData, err := base64.StdEncoding.DecodeString(cipherText)
	if err != nil {
		return "", err
	}

	blockSize := block.BlockSize()
	iv := cipherData[:blockSize]
	ciphertext := cipherData[blockSize:]

	mode := cipher.NewCBCDecrypter(block, iv)
	decrypted := make([]byte, len(ciphertext))
	mode.CryptBlocks(decrypted, ciphertext)

	return string(unpad(decrypted)), nil
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

	// Encryption key (must be between 4 to 56 bytes for Blowfish)
	encryptionKey := []byte("mysecretblowfishkey")

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
	insertStmt := `INSERT INTO lineitem6m_enc_Blowfish (
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

		err := rows.Scan(&L_ORDERKEY, &L_PARTKEY, &L_SUPPKEY, &L_LINENUMBER, &L_QUANTITY,
			&L_EXTENDEDPRICE, &L_DISCOUNT, &L_TAX, &L_RETURNFLAG, &L_LINESTATUS,
			&L_SHIPDATE, &L_COMMITDATE, &L_RECEIPTDATE, &L_SHIPINSTRUCT,
			&L_SHIPMODE, &L_COMMENT)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		// Encrypt each column except L_ORDERKEY
		encryptedL_PARTKEY, _ := encryptBlowfish(fmt.Sprintf("%d", L_PARTKEY), encryptionKey)
		encryptedL_SUPPKEY, _ := encryptBlowfish(fmt.Sprintf("%d", L_SUPPKEY), encryptionKey)
		encryptedL_LINENUMBER, _ := encryptBlowfish(fmt.Sprintf("%d", L_LINENUMBER), encryptionKey)
		encryptedL_QUANTITY, _ := encryptBlowfish(fmt.Sprintf("%f", L_QUANTITY), encryptionKey)
		encryptedL_EXTENDEDPRICE, _ := encryptBlowfish(fmt.Sprintf("%f", L_EXTENDEDPRICE), encryptionKey)
		encryptedL_DISCOUNT, _ := encryptBlowfish(fmt.Sprintf("%f", L_DISCOUNT), encryptionKey)
		encryptedL_TAX, _ := encryptBlowfish(fmt.Sprintf("%f", L_TAX), encryptionKey)
		encryptedL_RETURNFLAG, _ := encryptBlowfish(L_RETURNFLAG, encryptionKey)
		encryptedL_LINESTATUS, _ := encryptBlowfish(L_LINESTATUS, encryptionKey)
		encryptedL_SHIPDATE, _ := encryptBlowfish(L_SHIPDATE, encryptionKey)
		encryptedL_COMMITDATE, _ := encryptBlowfish(L_COMMITDATE, encryptionKey)
		encryptedL_RECEIPTDATE, _ := encryptBlowfish(L_RECEIPTDATE, encryptionKey)
		encryptedL_SHIPINSTRUCT, _ := encryptBlowfish(L_SHIPINSTRUCT, encryptionKey)
		encryptedL_SHIPMODE, _ := encryptBlowfish(L_SHIPMODE, encryptionKey)
		encryptedL_COMMENT, _ := encryptBlowfish(L_COMMENT, encryptionKey)

		_, err = stmt.Exec(L_ORDERKEY, encryptedL_PARTKEY, encryptedL_SUPPKEY, encryptedL_LINENUMBER, encryptedL_QUANTITY,
			encryptedL_EXTENDEDPRICE, encryptedL_DISCOUNT, encryptedL_TAX, encryptedL_RETURNFLAG, encryptedL_LINESTATUS,
			encryptedL_SHIPDATE, encryptedL_COMMITDATE, encryptedL_RECEIPTDATE, encryptedL_SHIPINSTRUCT,
			encryptedL_SHIPMODE, encryptedL_COMMENT)
		if err != nil {
			log.Fatal("Failed to insert into destination database:", err)
		}

		ct += 1
		if ct%10000 == 0 {
			fmt.Printf("Inserting : %v \n", ct)
		}
	}

	// Calculate total elapsed time
	totalDuration := time.Since(startTime)
	fmt.Printf("Total data transfer time: %s\n", totalDuration)
}

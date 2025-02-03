package main

import (
	"crypto/cipher"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/twofish"
)

// Fixed IV for Twofish (must be exactly 16 bytes)
var fixedIV = []byte("this_is_16_bytes") // 16 bytes IV

// Function to remove padding from decrypted plaintext
func unpad(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, fmt.Errorf("empty input")
	}

	padding := int(src[len(src)-1])
	if padding > len(src) || padding == 0 {
		return nil, fmt.Errorf("invalid padding")
	}

	for i := len(src) - padding; i < len(src); i++ {
		if int(src[i]) != padding {
			return nil, fmt.Errorf("invalid padding bytes")
		}
	}

	return src[:len(src)-padding], nil
}

// Function to decrypt a string using Twofish in CBC mode with a fixed IV
func decrypt(encryptedText string, key []byte) (string, error) {
	// Decode base64 encoded ciphertext
	cipherText, err := base64.URLEncoding.DecodeString(encryptedText)
	if err != nil {
		return "", err
	}

	// Create Twofish cipher
	block, err := twofish.NewCipher(key)
	if err != nil {
		return "", err
	}

	// Ensure the ciphertext length is a valid multiple of the block size
	blockSize := block.BlockSize()
	if len(cipherText)%blockSize != 0 {
		return "", fmt.Errorf("ciphertext is not a multiple of the block size")
	}

	// Create CBC mode decrypter using the fixed IV
	mode := cipher.NewCBCDecrypter(block, fixedIV)
	plainText := make([]byte, len(cipherText))
	mode.CryptBlocks(plainText, cipherText)

	// Remove padding and return the plaintext
	unpaddedText, err := unpad(plainText)
	if err != nil {
		return "", err
	}

	return string(unpaddedText), nil
}

func main() {
	// MySQL database connection details
	dsn := "root:Sabbir@123@tcp(127.0.0.1:3306)/acc"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Failed to connect to MySQL database:", err)
	}
	defer db.Close()

	// Encryption key (must be the same as the one used for encryption)
	encryptionKey := []byte("this_is_a_32_byte_twofish_key!!!") // 32-byte key

	// Query to read data from the encrypted lineitem table
	query := `SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, 
	                 L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, 
	                 L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, 
	                 L_SHIPMODE, L_COMMENT FROM lineitem1m_enc_Twofish`

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal("Failed to execute query:", err)
	}
	defer rows.Close()

	// Measure total time for the operation
	startTime := time.Now()
	ct := 1

	for rows.Next() {
		var (
			L_ORDERKEY                                                                                                     int
			encryptedL_PARTKEY, encryptedL_SUPPKEY, encryptedL_LINENUMBER                                                  string
			encryptedL_QUANTITY, encryptedL_EXTENDEDPRICE, encryptedL_DISCOUNT, encryptedL_TAX                             string
			encryptedL_RETURNFLAG, encryptedL_LINESTATUS, encryptedL_SHIPINSTRUCT, encryptedL_SHIPMODE, encryptedL_COMMENT string
			encryptedL_SHIPDATE, encryptedL_COMMITDATE, encryptedL_RECEIPTDATE                                             string
		)

		err := rows.Scan(
			&L_ORDERKEY, &encryptedL_PARTKEY, &encryptedL_SUPPKEY, &encryptedL_LINENUMBER,
			&encryptedL_QUANTITY, &encryptedL_EXTENDEDPRICE, &encryptedL_DISCOUNT, &encryptedL_TAX,
			&encryptedL_RETURNFLAG, &encryptedL_LINESTATUS, &encryptedL_SHIPDATE, &encryptedL_COMMITDATE,
			&encryptedL_RECEIPTDATE, &encryptedL_SHIPINSTRUCT, &encryptedL_SHIPMODE, &encryptedL_COMMENT,
		)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		// Decrypt each column
		L_PARTKEY, _ := decrypt(encryptedL_PARTKEY, encryptionKey)
		L_SUPPKEY, _ := decrypt(encryptedL_SUPPKEY, encryptionKey)
		L_LINENUMBER, _ := decrypt(encryptedL_LINENUMBER, encryptionKey)
		L_QUANTITY, _ := decrypt(encryptedL_QUANTITY, encryptionKey)
		L_EXTENDEDPRICE, _ := decrypt(encryptedL_EXTENDEDPRICE, encryptionKey)
		L_DISCOUNT, _ := decrypt(encryptedL_DISCOUNT, encryptionKey)
		L_TAX, _ := decrypt(encryptedL_TAX, encryptionKey)
		L_RETURNFLAG, _ := decrypt(encryptedL_RETURNFLAG, encryptionKey)
		L_LINESTATUS, _ := decrypt(encryptedL_LINESTATUS, encryptionKey)
		L_SHIPDATE, _ := decrypt(encryptedL_SHIPDATE, encryptionKey)
		L_COMMITDATE, _ := decrypt(encryptedL_COMMITDATE, encryptionKey)
		L_RECEIPTDATE, _ := decrypt(encryptedL_RECEIPTDATE, encryptionKey)
		L_SHIPINSTRUCT, _ := decrypt(encryptedL_SHIPINSTRUCT, encryptionKey)
		L_SHIPMODE, _ := decrypt(encryptedL_SHIPMODE, encryptionKey)
		L_COMMENT, _ := decrypt(encryptedL_COMMENT, encryptionKey)

		// Combine values for easy display
		value1 := L_PARTKEY + L_SUPPKEY + L_LINENUMBER + L_QUANTITY + L_EXTENDEDPRICE + L_DISCOUNT + L_TAX + L_RECEIPTDATE + L_SHIPMODE + L_COMMENT + L_SHIPINSTRUCT + L_RETURNFLAG + L_LINESTATUS + L_SHIPDATE + L_COMMITDATE

		fmt.Printf("Decryption row: %s\n", value1)

		ct += 1
		if ct%10000 == 0 {
			fmt.Printf("Decrypted : %v \n", ct)
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal("Error encountered during rows iteration:", err)
	}

	// Calculate total elapsed time
	totalDuration := time.Since(startTime)
	fmt.Printf("Total data processing time: %s\n", totalDuration)
}

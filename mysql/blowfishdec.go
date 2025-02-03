package main

import (
	"crypto/cipher"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/blowfish"
)

// Unpad removes padding after decryption
func unpad(src []byte) []byte {
	length := len(src)
	padding := int(src[length-1])
	return src[:length-padding]
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
	dsn := "root:Sabbir@123@tcp(127.0.0.1:3306)/acc"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Failed to connect to MySQL database:", err)
	}
	defer db.Close()

	// Encryption key (must be the same as the one used for encryption)
	encryptionKey := []byte("mysecretblowfishkey")

	// Query to read data from the encrypted lineitem table
	query := `SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, 
		         L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, 
		         L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, 
		         L_SHIPMODE, L_COMMENT FROM lineitem6m_enc_Blowfish`

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
		L_PARTKEY, _ := decryptBlowfish(encryptedL_PARTKEY, encryptionKey)
		L_SUPPKEY, _ := decryptBlowfish(encryptedL_SUPPKEY, encryptionKey)
		L_LINENUMBER, _ := decryptBlowfish(encryptedL_LINENUMBER, encryptionKey)
		L_QUANTITY, _ := decryptBlowfish(encryptedL_QUANTITY, encryptionKey)
		L_EXTENDEDPRICE, _ := decryptBlowfish(encryptedL_EXTENDEDPRICE, encryptionKey)
		L_DISCOUNT, _ := decryptBlowfish(encryptedL_DISCOUNT, encryptionKey)
		L_TAX, _ := decryptBlowfish(encryptedL_TAX, encryptionKey)
		L_RETURNFLAG, _ := decryptBlowfish(encryptedL_RETURNFLAG, encryptionKey)
		L_LINESTATUS, _ := decryptBlowfish(encryptedL_LINESTATUS, encryptionKey)
		L_SHIPDATE, _ := decryptBlowfish(encryptedL_SHIPDATE, encryptionKey)
		L_COMMITDATE, _ := decryptBlowfish(encryptedL_COMMITDATE, encryptionKey)
		L_RECEIPTDATE, _ := decryptBlowfish(encryptedL_RECEIPTDATE, encryptionKey)
		L_SHIPINSTRUCT, _ := decryptBlowfish(encryptedL_SHIPINSTRUCT, encryptionKey)
		L_SHIPMODE, _ := decryptBlowfish(encryptedL_SHIPMODE, encryptionKey)
		L_COMMENT, _ := decryptBlowfish(encryptedL_COMMENT, encryptionKey)

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

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
	"github.com/syndtr/goleveldb/leveldb"
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
	sourceDSN := "root:Sabbir@123@tcp(127.0.0.1:3306)/tpch"
	sourceDB, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		log.Fatal("Failed to connect to MySQL database:", err)
	}
	defer sourceDB.Close()

	// LevelDB connection details
	levelDB, err := leveldb.OpenFile("enc_database6m1", nil)
	if err != nil {
		log.Fatal("Failed to open LevelDB:", err)
	}
	defer levelDB.Close()

	// Encryption key (must be 16, 24, or 32 bytes long for AES-128, AES-192, or AES-256)
	encryptionKey := []byte("this_is_a_32_byte_encryption_key")

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

		// Encrypt each column except L_ORDERKEY
		// encryptedL_PARTKEY, _ := encrypt(fmt.Sprintf("%d", L_PARTKEY), encryptionKey)
		// encryptedL_SUPPKEY, _ := encrypt(fmt.Sprintf("%d", L_SUPPKEY), encryptionKey)
		// encryptedL_LINENUMBER, _ := encrypt(fmt.Sprintf("%d", L_LINENUMBER), encryptionKey)
		// encryptedL_QUANTITY, _ := encrypt(fmt.Sprintf("%f", L_QUANTITY), encryptionKey)
		// encryptedL_EXTENDEDPRICE, _ := encrypt(fmt.Sprintf("%f", L_EXTENDEDPRICE), encryptionKey)
		// encryptedL_DISCOUNT, _ := encrypt(fmt.Sprintf("%f", L_DISCOUNT), encryptionKey)
		// encryptedL_TAX, _ := encrypt(fmt.Sprintf("%f", L_TAX), encryptionKey)
		// encryptedL_RETURNFLAG, _ := encrypt(L_RETURNFLAG, encryptionKey)
		// encryptedL_LINESTATUS, _ := encrypt(L_LINESTATUS, encryptionKey)
		// encryptedL_SHIPDATE, _ := encrypt(L_SHIPDATE, encryptionKey)
		// encryptedL_COMMITDATE, _ := encrypt(L_COMMITDATE, encryptionKey)
		// encryptedL_RECEIPTDATE, _ := encrypt(L_RECEIPTDATE, encryptionKey)
		// encryptedL_SHIPINSTRUCT, _ := encrypt(L_SHIPINSTRUCT, encryptionKey)
		// encryptedL_SHIPMODE, _ := encrypt(L_SHIPMODE, encryptionKey)
		// encryptedL_COMMENT, _ := encrypt(L_COMMENT, encryptionKey)

		// fmt.Printf("%d\n", L_PARTKEY)

		// fmt.Printf("%d\n", L_SUPPKEY)
		// fmt.Printf("\n\n encryptedL_PARTKEY : %s, L_PARTKEY: %d\n", encryptedL_PARTKEY, L_PARTKEY)
		// fmt.Printf("\n\n encryptedL_LINENUMBER : %s, L_SUPPKEY: %d\n", encryptedL_LINENUMBER, L_SUPPKEY)

		// fmt.Printf("\n\n encryptionKey : %s, \n", encryptionKey)

		// fmt.Printf("\n\n L_COMMENT : %s, \n", L_COMMENT)

		// fmt.Printf("\n\n encryptedL_PARTKEY : %s, encryptedL_SUPPKEY: %s\n", encryptedL_PARTKEY, encryptedL_SUPPKEY)
		// fmt.Printf("\n\n encryptedL_LINENUMBER : %s, encryptedL_QUANTITY: %s\n", encryptedL_LINENUMBER, encryptedL_QUANTITY)

		// Measure time for each insert into LevelDB
		//startInsert := time.Now()
		key := fmt.Sprintf("%d_%d_%d_%d", L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)

		value1 := L_RECEIPTDATE + L_SHIPMODE + L_COMMENT + L_SHIPINSTRUCT + L_RETURNFLAG + L_LINESTATUS + L_SHIPDATE + L_COMMITDATE
		encryptedL_value, _ := encrypt(value1, encryptionKey)

		// value := fmt.Sprintf("%s %s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s",
		// 	encryptedL_PARTKEY, encryptedL_SUPPKEY, encryptedL_LINENUMBER,
		// 	encryptedL_QUANTITY, encryptedL_EXTENDEDPRICE, encryptedL_DISCOUNT,
		// 	encryptedL_TAX, encryptedL_RETURNFLAG, encryptedL_LINESTATUS,
		// 	encryptedL_SHIPDATE, encryptedL_COMMITDATE, encryptedL_RECEIPTDATE,
		// 	encryptedL_SHIPINSTRUCT, encryptedL_SHIPMODE, encryptedL_COMMENT)

		//	fmt.Printf("\n\nInserting.....  Key: %s, Value: %s\n", key, value)
		err = levelDB.Put([]byte(key), []byte(encryptedL_value), nil)
		if err != nil {
			log.Fatal("Failed to insert into LevelDB:", err)
		}
		// insertDuration := time.Since(startInsert)
		// fmt.Printf("LevelDB insert time: %s\n", insertDuration)

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

	//--------------------print leveldb test data--------------

	// ct = 0
	// iter := levelDB.NewIterator(nil, nil)
	// for iter.Next() {
	// 	key := iter.Key()
	// 	value := iter.Value()
	// 	ct += 1
	// 	fmt.Printf("\n\nKey: %s, Value: %s\n", key, value)
	// 	if ct == 5 {
	// 		break
	// 	}

	// }

}

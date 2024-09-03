package main

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

// Function to decrypt a string using AES encryption
func decrypt(encryptedText string, key []byte) (string, error) {
	cipherText, _ := base64.URLEncoding.DecodeString(encryptedText)

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	if len(cipherText) < aes.BlockSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	iv := cipherText[:aes.BlockSize]
	cipherText = cipherText[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(cipherText, cipherText)

	return string(cipherText), nil
}

func main() {
	// LevelDB connection details
	db, err := leveldb.OpenFile("enc_database6m1", nil)
	if err != nil {
		log.Fatal("Failed to open LevelDB:", err)
	}
	defer db.Close()

	// Encryption key (must be the same as the one used for encryption)
	encryptionKey := []byte("this_is_a_32_byte_encryption_key")

	// Measure total time for the operation
	startTime := time.Now()
	ct := 0
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		// Retrieve the key and encrypted value
		key := string(iter.Key())
		encryptedValue := string(iter.Value())

		//fmt.Printf("encryptedValue value =%s\n", key, encryptedValue)

		// Split the encrypted value into its components
		//encryptedFields := splitEncryptedFields(encryptedValue)

		// Measure time for each decryption
		//startDecrypt := time.Now()
		// L_PARTKEY, _ := decrypt(encryptedFields[0], encryptionKey)
		// L_SUPPKEY, _ := decrypt(encryptedFields[1], encryptionKey)
		// L_LINENUMBER, _ := decrypt(encryptedFields[2], encryptionKey)
		// L_QUANTITY, _ := decrypt(encryptedFields[3], encryptionKey)
		// L_EXTENDEDPRICE, _ := decrypt(encryptedFields[4], encryptionKey)
		// L_DISCOUNT, _ := decrypt(encryptedFields[5], encryptionKey)
		// L_TAX, _ := decrypt(encryptedFields[6], encryptionKey)
		// L_RETURNFLAG, _ := decrypt(encryptedFields[7], encryptionKey)
		// L_LINESTATUS, _ := decrypt(encryptedFields[8], encryptionKey)
		// L_SHIPDATE, _ := decrypt(encryptedFields[9], encryptionKey)
		// L_COMMITDATE, _ := decrypt(encryptedFields[10], encryptionKey)
		// L_RECEIPTDATE, _ := decrypt(encryptedFields[11], encryptionKey)
		// L_SHIPINSTRUCT, _ := decrypt(encryptedFields[12], encryptionKey)
		// L_SHIPMODE, _ := decrypt(encryptedFields[13], encryptionKey)
		//L_COMMENT, _ := decrypt(encryptedFields[14], encryptionKey)
		//decryptDuration := time.Since(startDecrypt)

		decryptedFields, _ := decrypt(encryptedValue, encryptionKey)

		//L_PARTKEY += L_SUPPKEY + L_LINENUMBER + L_QUANTITY + L_RECEIPTDATE + L_SHIPMODE + L_COMMENT + L_SHIPINSTRUCT + L_EXTENDEDPRICE + L_DISCOUNT + L_TAX + L_RETURNFLAG + L_LINESTATUS + L_SHIPDATE + L_COMMITDATE

		fmt.Printf("Decrypted row for key %s: value =%s\n", key, decryptedFields)

		ct += 1
		if ct%10000 == 0 {
			fmt.Printf("Decrypted : %v \n", ct)
		}
		// if ct == 3 {
		// 	break
		// }

	}
	iter.Release()

	if err := iter.Error(); err != nil {
		log.Fatal("Error during iteration:", err)
	}

	// Calculate total elapsed time
	totalDuration := time.Since(startTime)
	fmt.Printf("Total data processing time: %s\n", totalDuration)
}

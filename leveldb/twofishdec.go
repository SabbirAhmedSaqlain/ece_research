package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/crypto/twofish"
)

// Function to remove padding from decrypted text
func unpad(src []byte) []byte {
	length := len(src)
	padding := int(src[length-1])
	return src[:length-padding]
}

// Function to decrypt a string using Twofish
func decryptTwofish(encryptedText string, key []byte) (string, error) {
	cipherText, err := base64.URLEncoding.DecodeString(encryptedText)
	if err != nil {
		return "", err
	}

	block, err := twofish.NewCipher(key)
	if err != nil {
		return "", err
	}

	blockSize := block.BlockSize()
	if len(cipherText) < blockSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	decrypted := make([]byte, len(cipherText))
	for i := 0; i < len(cipherText); i += blockSize {
		block.Decrypt(decrypted[i:], cipherText[i:])
	}

	// Remove padding and return the decrypted string
	return string(unpad(decrypted)), nil
}

func main() {
	// LevelDB connection details
	db, err := leveldb.OpenFile("enc_database6m", nil)
	if err != nil {
		log.Fatal("Failed to open LevelDB:", err)
	}
	defer db.Close()

	// Encryption key (must be the same as the one used for encryption)
	encryptionKey := []byte("thisisaverysecurekeyfor2fish!") // 32-byte key for Twofish-256

	// Measure total time for the operation
	startTime := time.Now()
	ct := 0
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		// Retrieve the key and encrypted value
		key := string(iter.Key())
		encryptedValue := string(iter.Value())

		// Decrypt the value using Twofish
		decryptedFields, err := decryptTwofish(encryptedValue, encryptionKey)
		if err != nil {
			log.Printf("Failed to decrypt row for key %s: %v\n", key, err)
			continue
		}

		// Print the decrypted data
		fmt.Printf("Decrypted row for key %s: value = %s\n", key, decryptedFields)

		ct += 1
		if ct%10000 == 0 {
			fmt.Printf("Decrypted : %v \n", ct)
		}
	}

	iter.Release()
	if err := iter.Error(); err != nil {
		log.Fatal("Error during iteration:", err)
	}

	// Calculate total elapsed time
	totalDuration := time.Since(startTime)
	fmt.Printf("Total data processing time: %s\n", totalDuration)
}

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
	decrypted := make([]byte, len(cipherText))
	for i := 0; i < len(cipherText); i += blockSize {
		block.Decrypt(decrypted[i:], cipherText[i:])
	}

	// Remove padding and return the decrypted string
	return string(unpad(decrypted)), nil
}

func main() {
	// Open LevelDB
	db, err := leveldb.OpenFile("enc_database6mtwofish", nil)
	if err != nil {
		log.Fatal("Failed to open LevelDB:", err)
	}
	defer db.Close()

	// FIXED: Proper 32-byte encryption key
	encryptionKey := []byte("this_is_a_very_secure_32byte_key") // EXACTLY 32 bytes
	startTime := time.Now()
	// Iterate through LevelDB
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		encryptedValue := string(iter.Value())

		decryptedFields, err := decryptTwofish(encryptedValue, encryptionKey)
		if err != nil {
			log.Printf("Decryption failed for key %s: %v\n", key, err)
			continue
		}

		fmt.Printf("Decrypted row for key %s: value = %s\n", key, decryptedFields)
	}
	totalDuration := time.Since(startTime)
	fmt.Printf("Total data transfer time: %s\n", totalDuration)

	iter.Release()
}

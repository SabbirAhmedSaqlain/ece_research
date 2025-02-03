package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/crypto/chacha20"
)

var nonce = []byte("123456789012") // 12-byte nonce

// Function to decrypt a string using ChaCha20
func decryptChaCha20(encryptedText string, key []byte) (string, error) {
	cipherText, err := base64.URLEncoding.DecodeString(encryptedText)
	if err != nil {
		return "", err
	}

	cipher, err := chacha20.NewUnauthenticatedCipher(key, nonce)
	if err != nil {
		return "", err
	}

	decrypted := make([]byte, len(cipherText))
	cipher.XORKeyStream(decrypted, cipherText)

	return string(decrypted), nil
}

func main() {
	// Open LevelDB
	db, err := leveldb.OpenFile("enc_database6mcha", nil)
	if err != nil {
		log.Fatal("Failed to open LevelDB:", err)
	}
	defer db.Close()

	// FIXED: Use a proper 32-byte encryption key
	encryptionKey := []byte("this_is_a_32_byte_encryption_key") // EXACTLY 32 bytes

	// Measure processing time
	startTime := time.Now()
	ct := 0

	// Iterate through LevelDB
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		encryptedValue := string(iter.Value())

		// Perform ChaCha20 decryption
		decryptedFields, err := decryptChaCha20(encryptedValue, encryptionKey)
		if err != nil {
			log.Printf("Decryption failed for key %s: %v\n", key, err)
			continue
		}

		fmt.Printf("Decrypted row for key %s: value = %s\n", key, decryptedFields)

		ct++
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

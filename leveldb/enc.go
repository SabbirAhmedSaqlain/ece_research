package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"log"
)

// Function to encrypt a string using AES encryption
func encrypt(plainText string, key []byte) (string, error) {
	// Create a new AES cipher using the key
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
	// The data to encrypt
	plainText := "12345sabbir"

	// AES encryption key (must be 16, 24, or 32 bytes long)
	key := []byte("this_is_a_32_byte_encryption_key")

	// Encrypt the data
	encryptedText, err := encrypt(plainText, key)
	if err != nil {
		log.Fatal("Failed to encrypt data:", err)
	}

	// Print the encrypted data
	fmt.Println("Encrypted text:", encryptedText)
}

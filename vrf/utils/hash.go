package utils

import (
    "golang.org/x/crypto/sha3"
)

// Sha3256 returns the SHA3-256 digest of the data.
func Sha3256(args ...[]byte) []byte {
	hasher := sha3.New256()
	for _, bytes := range args {
		hasher.Write(bytes)
	}
	return hasher.Sum(nil)
}

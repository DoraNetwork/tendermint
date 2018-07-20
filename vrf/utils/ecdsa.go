package utils

import (
	"crypto/ecdsa"
	"crypto/elliptic"

	"encoding/hex"
	"errors"
	"math/big"
)

// FromECDSAPrivateKey exports a private key into a binary dump.
func FromECDSAPrivateKey(priv *ecdsa.PrivateKey) ([]byte, error) {
	if priv == nil {
		return nil, errors.New("ecdsa: please input private key")
	}
	// as private key len cannot guarantee greater than Params bytes len, padding big bytes.
	return paddedBigBytes(priv.D, priv.Params().BitSize/8), nil
}

// FromECDSAPublicKey exports a public key into a binary dump.
func FromECDSAPublicKey(curve elliptic.Curve, pub *ecdsa.PublicKey) ([]byte, error) {
	if pub == nil || pub.X == nil || pub.Y == nil {
		return nil, errors.New("ecdsa: please input public key")
	}
	return elliptic.Marshal(curve, pub.X, pub.Y), nil
}

// HexToECDSAPrivateKey parses a secp256k1 private key.
func HexToECDSAPrivateKey(curve elliptic.Curve, hexkey string) (*ecdsa.PrivateKey, error) {
	b, err := hex.DecodeString(hexkey)
	if err != nil {
		return nil, err
	}
	return ToECDSAPrivateKey(curve, b)
}

// ToECDSAPrivateKey creates a private key with the given data value.
func ToECDSAPrivateKey(curve elliptic.Curve, d []byte) (*ecdsa.PrivateKey, error) {
	priv := new(ecdsa.PrivateKey)
	priv.PublicKey.Curve = curve
	priv.D = new(big.Int).SetBytes(d)
	priv.PublicKey.X, priv.PublicKey.Y = priv.PublicKey.Curve.ScalarBaseMult(d)
	return priv, nil
}

// ToECDSAPublicKey creates a public key with the given data value.
func ToECDSAPublicKey(curve elliptic.Curve, pub []byte) (*ecdsa.PublicKey, error) {
	if len(pub) == 0 {
		return nil, errors.New("ecdsa: please input public key bytes")
	}
	x, y := elliptic.Unmarshal(curve, pub)
	return &ecdsa.PublicKey{Curve: curve, X: x, Y: y}, nil
}

// zeroKey zeroes the private key
func zeroKey(k *ecdsa.PrivateKey) {
	b := k.D.Bits()
	for i := range b {
		b[i] = 0
	}
}

// paddedBigBytes encodes a big integer as a big-endian byte slice.
func paddedBigBytes(bigint *big.Int, n int) []byte {
	if bigint.BitLen()/8 >= n {
		return bigint.Bytes()
	}
	ret := make([]byte, n)
	readBits(bigint, ret)
	return ret
}

const (
	// number of bits in a big.Word
	wordBits = 32 << (uint64(^big.Word(0)) >> 63)
	// number of bytes in a big.Word
	wordBytes = wordBits / 8
)

// readBits encodes the absolute value of bigint as big-endian bytes.
func readBits(bigint *big.Int, buf []byte) {
	i := len(buf)
	for _, d := range bigint.Bits() {
		for j := 0; j < wordBytes && i > 0; j++ {
			i--
			buf[i] = byte(d)
			d >>= 8
		}
	}
}

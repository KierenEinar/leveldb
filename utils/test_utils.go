package utils

import (
	"math/rand"
	"time"
)

var (
	rnd        = rand.New(rand.NewSource(time.Now().Unix()))
	characters = make([]rune, 0, 62)
	hex        = []rune("0123456789abcdef")
)

func init() {
	minAsciiUpper := uint8('A')
	minAsciiLower := uint8('a')
	for i := uint8(0); i < 26; i++ {
		characters = append(characters, rune(minAsciiLower+i))
	}
	for i := uint8(0); i < 26; i++ {
		characters = append(characters, rune(minAsciiUpper+i))
	}
	for i := 0; i < 10; i++ {
		characters = append(characters, rune(i))
	}
}

func RandString(maxLen int) string {
	randLen := rnd.Int()%maxLen + 1
	r := make([]rune, 0, randLen)
	for i := 0; i < randLen; i++ {
		randPos := rnd.Int() % len(characters)
		r = append(r, characters[randPos])
	}
	return string(r)
}

func RandHex(maxLen int) string {
	randLen := rnd.Int()%maxLen + 1
	r := make([]rune, 0, randLen)
	for i := 0; i < randLen; i++ {
		randPos := rnd.Int() % len(hex)
		r = append(r, hex[randPos])
	}
	return string(r)
}

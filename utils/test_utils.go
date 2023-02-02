package utils

import (
	"math/rand"
	"time"
)

var (
	rnd        = rand.New(rand.NewSource(time.Now().Unix()))
	characters = make([]rune, 0, 52)
)

func init() {
	minAsciiUpper := uint8('A')
	minAsciiLower := uint8('a')
	for i := uint8(0); i < 26; i++ {
		characters = append(characters, rune(minAsciiUpper+i))
	}
	for i := uint8(0); i < 26; i++ {
		characters = append(characters, rune(minAsciiLower+i))
	}
}

func RandRunes(maxLen int) []rune {
	randLen := rnd.Int()%maxLen + 1
	r := make([]rune, 0, randLen)
	for i := 0; i < randLen; i++ {
		randPos := rnd.Int() % len(characters)
		r = append(r, characters[randPos])
	}
	return r
}

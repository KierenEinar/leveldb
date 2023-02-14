package utils

import (
	"math/rand"
	"time"
)

var (
	rnd    = rand.New(rand.NewSource(time.Now().Unix()))
	letter = make([]rune, 0, 62)
	hex    = []rune("0123456789abcdef")
)

func init() {
	minAsciiUpper := uint8('A')
	minAsciiLower := uint8('a')
	minAsciiNumber := uint8('0')
	for i := uint8(0); i < 26; i++ {
		letter = append(letter, rune(minAsciiLower+i))
	}
	for i := uint8(0); i < 26; i++ {
		letter = append(letter, rune(minAsciiUpper+i))
	}
	for i := uint8(0); i < 10; i++ {
		letter = append(letter, rune(minAsciiNumber+i))
	}
}

func RandString(maxLen int) string {
	randLen := rnd.Int()%maxLen + 1
	r := make([]rune, 0, randLen)
	for i := 0; i < randLen; i++ {
		randPos := rnd.Int() % len(letter)
		r = append(r, letter[randPos])
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

func RandStringByLen(size int) string {
	r := make([]rune, 0, size)
	for i := 0; i < size; i++ {
		randPos := rnd.Int() % len(letter)
		r = append(r, letter[randPos])
	}
	return string(r)
}

func RandHexByLen(size int) string {
	r := make([]rune, 0, size)
	for i := 0; i < size; i++ {
		randPos := rnd.Int() % len(hex)
		r = append(r, hex[randPos])
	}
	return string(r)
}

func LetterRunes() []rune {
	return letter
}

func LetterBytes() []byte {
	return []byte(string(letter))
}

func ForeachLetter(loopTimes int, f func(i int, c rune)) {
	for i := 0; i < loopTimes; i++ {
		for ix := 0; ix < len(letter); ix++ {
			f(ix, letter[ix])
		}
	}
}

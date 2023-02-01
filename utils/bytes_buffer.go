package utils

func PoolGetBytes(n int) []byte {
	// todo used sync.Pool
	bytes := make([]byte, n)
	return bytes
}

func PoolPutBytes(b []byte) {

}

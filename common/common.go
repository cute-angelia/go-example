package common

import (
	"fmt"
	"os"
)

func PanicError(msg string, err error) {
	fmt.Println("Error:", msg, err)
	os.Exit(-1)
}

func LogError(msg string, err error) {
	fmt.Println("Error:", msg, err)
}

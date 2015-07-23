package cuckoofilter

import (
	"bufio"
	"fmt"
	"os"
	"testing"
	"time"

	"code.google.com/p/gofarmhash"
)

func TestIndexAndFP(t *testing.T) {
	data := []byte("seif")
	i1, i2, fp := getIndicesAndFingerprint(data, 4096)
	i11 := getAltIndex(fp, i2, 4096)
	i22 := getAltIndex(fp, i1, 4096)
	if i1 != i11 {
		t.Errorf("Expected i1 == i11, instead %d != %d", i1, i11)
	}
	if i2 != i22 {
		t.Errorf("Expected i2 == i22, instead %d != %d", i2, i22)
	}
}

func TestFarmhash(t *testing.T) {
	fd, err := os.Open("/usr/share/dict/web2")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	scanner := bufio.NewScanner(fd)

	values := make([]string, 0)
	for scanner.Scan() {
		s := scanner.Text()
		values = append(values, s)
	}

	var tt time.Duration = 0
	for _, v := range values {
		tr := time.Now()
		farmhash.Hash64([]byte(v))
		tt += time.Since(tr)
	}
}

package cuckoofilter

const fingerprintSize = 1
const bucketSize = 4

type fingerprint [fingerprintSize]byte
type item struct{
	fp fingerprint
	addr uint32
}
type bucket [bucketSize]item

var nullFp = fingerprint{0}

func (b *bucket) insert(it item) bool {
	for i, item := range b {
		if item.fp == nullFp {
			b[i] = it
			return true
		}
	}
	return false
}

/*
func (b *bucket) delete(fp fingerprint) bool {
	for i, tfp := range b {
		if tfp == fp {
			b[i] = nullFp
			return true
		}
	}
	return false
}
*/

func (b *bucket) getFingerprintIndex(fp fingerprint) (index int,addr uint32){
	for i, it := range b {
		if it.fp == fp {
			return i,it.addr
		}
	}
	return -1,uint32(0)
}

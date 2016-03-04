package cuckoofilter

const fingerprintSize = 1
const bucketSize = 4

type Fingerprint [fingerprintSize]byte
type Item struct{
	Fp Fingerprint
	Addr uint32
}
type Bucket [bucketSize]Item

var nullFp = Fingerprint{0}


func (b *Bucket) insert(it Item) bool {
	for i, item := range b {
		if item.Fp == nullFp {
			b[i] = it
			return true
		}
	}
	return false
}

/*
func (b *Bucket) delete(fp Fingerprint) bool {
	for i, tfp := range b {
		if tfp == fp {
			b[i] = nullFp
			return true
		}
	}
	return false
}
*/

func (b *Bucket) getFingerprintIndex(fp Fingerprint) (index int,addr uint32){
	for i, it := range b {
		if it.Fp == fp {
			return i,it.Addr
		}
	}
	return -1,uint32(0)
}

package cuckoofilter

import (
	"os"
	"math/rand"
	"bufio"
	"encoding/binary"
)

const maxCuckooCount = 500

/*
CuckooFilter represents a probabalistic counter
*/

type CuckooFilter struct {
	file *os.File
	bucketStructSize uint
	bucketLength uint
	count   uint
}
func (f *CuckooFilter)getbucket(i uint)Bucket{
	_,err := f.file.Seek(int64(i*f.bucketStructSize),0)
	if err != nil{
		panic(err)
	}
	bucket := Bucket{}
	err = binary.Read(f.file,binary.BigEndian,&bucket)
	if err != nil{
		panic(err)
	}
	return bucket
}


/*
NewCuckooFilter returns a new cuckoofilter with a given capacity
*/
func NewCuckooFilter(capacity uint,filename string) *CuckooFilter {
	capacity = getNextPow2(uint64(capacity)) / bucketSize
	if capacity == 0 {
		capacity = 1
	}
	file,err := os.Create(filename)
	if err != nil{
		return nil
	}
	bucketStructSize := uint(binary.Size(Bucket{}))
	w := bufio.NewWriter(file)
	for i := uint(0);i<capacity*bucketStructSize;i++{
		w.Write([]byte{0})
	}
	w.Flush()
	return &CuckooFilter{
		bucketStructSize:bucketStructSize,
		file:file,
		count:0,
		bucketLength:capacity,
	}
}

/*
NewDefaultCuckooFilter returns a new cuckoofilter with the default capacity of 1000000
*/
func NewDefaultCuckooFilter(filename string) *CuckooFilter {
	return NewCuckooFilter(1000000,filename)
}

/*
Lookup returns true if data is in the counter
*/
func (cf *CuckooFilter) LookupAddr(key []byte)(uint32,bool) {
	i1, i2, fp := getIndicesAndFingerprint(key, uint(cf.bucketLength))
	b1, b2 := cf.getbucket(i1), cf.getbucket(i2)
	idx1,addr1 := b1.getFingerprintIndex(fp)
	if idx1 > -1{
		return addr1,true
	}
	idx2,addr2 := b2.getFingerprintIndex(fp)
	if idx2 > -1{
		return addr2,true
	}
	return 0,false
}

/*
Insert inserts data into the counter and returns true upon success
*/
func (cf *CuckooFilter) InsertAddr(key []byte,addr uint32) bool {
	i1, i2, fp := getIndicesAndFingerprint(key, uint(cf.bucketLength))
	it := Item{
		Fp:fp,
		Addr:addr,
	}
	bkt1 := cf.getbucket(i1)
	bkt2 := cf.getbucket(i2)
	if cf.insert(bkt1,it, i1) || cf.insert(bkt2,it, i2) {
		return true
	}
	return cf.reinsert(it, i2)
}

/*
InsertUnique inserts data into the counter if not exists and returns true upon success
*/
/*
func (cf *CuckooFilter) InsertUnique(data []byte) bool {
	if cf.Lookup(data) {
		return false
	}
	return cf.Insert(data)
}
*/

func (cf *CuckooFilter) insert(bkt Bucket,it Item, i uint) bool {
	if bkt.insert(it) {
		cf.count++
		cf.setBucket(i,bkt)
		return true
	}
	return false
}
func (cf *CuckooFilter) setBucket(i uint,bkt Bucket){
	_,err := cf.file.Seek(int64((i*cf.bucketStructSize)),0)
        if err != nil{
                panic(err)
        }       
        err = binary.Write(cf.file,binary.BigEndian,bkt)
        if err != nil{
                panic(err)
        }
}

func (cf *CuckooFilter) reinsert(it Item, i uint) bool {
	bkt := cf.getbucket(i)
	for k := 0; k < maxCuckooCount; k++ {
		j := rand.Intn(bucketSize)
		next := bkt[j]
		bkt[j] = it
		cf.setBucket(i,bkt)

		// look in the alternate location for that random element
		it = next
		i = getAltIndex(it.Fp, i, uint(cf.bucketLength))
		bkt = cf.getbucket(i)
		if cf.insert(bkt,it, i) {
			return true
		}
	}
	return false
}

/*
Delete data from counter if exists and return if deleted or not
*/
/*
func (cf *CuckooFilter) Delete(data []byte) bool {
	i1, i2, fp := getIndicesAndFingerprint(data, uint(len(cf.buckets)))
	return cf.delete(fp, i1) || cf.delete(fp, i2)
}

func (cf *CuckooFilter) delete(fp Fingerprint, i uint) bool {
	if cf.buckets[i].delete(fp) {
		cf.count--
		return true
	}
	return false
}
*/
/*
GetCount returns the number of items in the counter
*/
func (cf *CuckooFilter) Count() uint {
	return cf.count
}

package cuckoofilter

import "math/rand"

const maxCuckooCount = 500

/*
CuckooFilter represents a probabalistic counter
*/
type CuckooFilter struct {
	buckets []bucket
	count   uint
}

/*
NewCuckooFilter returns a new cuckoofilter with a given capacity
*/
func NewCuckooFilter(capacity uint) *CuckooFilter {
	capacity = getNextPow2(uint64(capacity)) / bucketSize
	if capacity == 0 {
		capacity = 1
	}
	buckets := make([]bucket, capacity, capacity)
	for i := range buckets {
		buckets[i] = [bucketSize]item{}
	}
	return &CuckooFilter{buckets, 0}
}

/*
NewDefaultCuckooFilter returns a new cuckoofilter with the default capacity of 1000000
*/
func NewDefaultCuckooFilter() *CuckooFilter {
	return NewCuckooFilter(1000000)
}

/*
Lookup returns true if data is in the counter
*/
func (cf *CuckooFilter) LookupAddr(key []byte)(uint32,bool) {
	i1, i2, fp := getIndicesAndFingerprint(key, uint(len(cf.buckets)))
	b1, b2 := cf.buckets[i1], cf.buckets[i2]
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
	i1, i2, fp := getIndicesAndFingerprint(key, uint(len(cf.buckets)))
	it := item{
		fp:fp,
		addr:addr,
	}
	if cf.insert(it, i1) || cf.insert(it, i2) {
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

func (cf *CuckooFilter) insert(it item, i uint) bool {
	if cf.buckets[i].insert(it) {
		cf.count++
		return true
	}
	return false
}

func (cf *CuckooFilter) reinsert(it item, i uint) bool {
	for k := 0; k < maxCuckooCount; k++ {
		j := rand.Intn(bucketSize)
		next := cf.buckets[i][j]
		cf.buckets[i][j] = it

		// look in the alternate location for that random element
		it = next
		i = getAltIndex(it.fp, i, uint(len(cf.buckets)))
		if cf.insert(it, i) {
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

func (cf *CuckooFilter) delete(fp fingerprint, i uint) bool {
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

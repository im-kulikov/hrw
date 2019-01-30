// Package hrw implements Rendezvous hashing.
// http://en.wikipedia.org/wiki/Rendezvous_hashing.
package hrw

import (
	"encoding/binary"
	"errors"
	"reflect"
	"sort"
	"strconv"

	"github.com/reusee/mmh3"
)

type (
	swapper func(i, j int)

	Hasher interface{ Hash() uint64 }

	hashed struct {
		length int
		sorted []uint64
		weight []uint64
	}
)

func weight(x uint64, y uint64) uint64 {
	acc := x ^ y
	// here used mmh3 64 bit finalizer
	// https://github.com/aappleby/smhasher/blob/61a0530f28277f2e850bfc39600ce61d02b518de/src/MurmurHash3.cpp#L81
	acc ^= acc >> 33
	acc = acc * 0xff51afd7ed558ccd
	acc ^= acc >> 33
	acc = acc * 0xc4ceb9fe1a85ec53
	acc ^= acc >> 33
	return acc
}

func (h hashed) Len() int           { return h.length }
func (h hashed) Less(i, j int) bool { return h.weight[h.sorted[i]] < h.weight[h.sorted[j]] }
func (h hashed) Swap(i, j int)      { h.sorted[i], h.sorted[j] = h.sorted[j], h.sorted[i] }

func Hash(key []byte) uint64 {
	h := mmh3.New128()
	// error always nil
	_, _ = h.Write(key)
	data := h.Sum(nil)
	return weight(
		binary.BigEndian.Uint64(data[:8]),
		binary.BigEndian.Uint64(data[8:]))
}

func SortByWeight(nodes []uint64, hash uint64) []uint64 {
	var (
		l = len(nodes)
		h = hashed{
			length: l,
			sorted: make([]uint64, 0, l),
			weight: make([]uint64, 0, l),
		}
	)

	for i, node := range nodes {
		h.sorted = append(h.sorted, uint64(i))
		h.weight = append(h.weight, weight(node, hash))
	}

	sort.Sort(h)
	return h.sorted
}

func SortSliceByValue(slice interface{}, hash uint64) error {
	t := reflect.TypeOf(slice)
	if t.Kind() != reflect.Slice {
		return errors.New("must be slice")
	}

	var (
		val    = reflect.ValueOf(slice)
		swap   = reflect.Swapper(slice)
		length = val.Len()
		rule   = make([]uint64, 0, length)
	)

	if length == 0 {
		return nil
	}

	switch slice := slice.(type) {
	case []int:
		for i := 0; i < length; i++ {
			key := strconv.Itoa(slice[i])
			h := []byte(key)
			// panic(Hash(h))
			rule = append(rule, weight(hash, Hash(h)))
		}
	case []string:
		for i := 0; i < length; i++ {
			rule = append(rule, weight(hash,
				Hash([]byte(slice[i]))))
		}
	default:
		if _, ok := val.Index(0).Interface().(Hasher); !ok {
			return errors.New("unknown type")
		}

		for i := 0; i < length; i++ {
			h := val.Index(i).Interface().(Hasher)
			rule = append(rule, weight(hash, h.Hash()))
		}
	}

	rule = SortByWeight(rule, hash)
	sortByRule(swap, uint64(length), rule)

	return nil
}

func SortSliceByIndex(slice interface{}, hash uint64) {
	length := uint64(reflect.ValueOf(slice).Len())
	swap := reflect.Swapper(slice)

	rule := make([]uint64, 0, length)
	for i := uint64(0); i < length; i++ {
		rule = append(rule, i)
	}

	rule = SortByWeight(rule, hash)
	sortByRule(swap, length, rule)
}

func sortByRule(swap swapper, length uint64, rule []uint64) {
	done := make([]bool, length)
	for i := uint64(0); i < length; i++ {
		if done[i] {
			continue
		}

		done[i] = true

		for j := rule[i]; !done[rule[j]]; j = rule[j] {
			swap(int(i), int(j))
			done[j] = true
		}
	}
}

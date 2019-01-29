// Package hrw implements Rendezvous hashing.
// http://en.wikipedia.org/wiki/Rendezvous_hashing.
package hrw

import (
	"errors"
	"hash/fnv"
	"reflect"
	"sort"
	"strconv"
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

const m64 = 18446744073709551615 // modulus (2**64-1)

func weight(x uint64, y uint64) uint64 {
	acc := x ^ y
	acc ^= acc >> 33
	acc = (acc * 0xff51afd7ed558ccd) % m64
	acc ^= acc >> 33
	acc = (acc * 0xc4ceb9fe1a85ec53) % m64
	acc ^= acc >> 33
	return acc
}

func (h hashed) Len() int           { return h.length }
func (h hashed) Less(i, j int) bool { return h.weight[h.sorted[i]] < h.weight[h.sorted[j]] }
func (h hashed) Swap(i, j int)      { h.sorted[i], h.sorted[j] = h.sorted[j], h.sorted[i] }

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
		hasher := fnv.New64()
		for i := 0; i < length; i++ {
			hasher.Reset()
			// error always nil
			_, _ = hasher.Write([]byte(strconv.Itoa(slice[i])))
			rule = append(rule, weight(hash, hasher.Sum64()))
		}
	case []string:
		hasher := fnv.New64()
		for i := 0; i < length; i++ {
			hasher.Reset()
			// error always nil
			_, _ = hasher.Write([]byte(slice[i]))
			rule = append(rule, weight(hash, hasher.Sum64()))
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

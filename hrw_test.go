package hrw

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
	"testing"
)

type hashString string

var testKey = []byte("0xff51afd7ed558ccd")

func Example() {
	// given a set of servers
	servers := []string{
		"one.example.com",
		"two.example.com",
		"three.example.com",
		"four.example.com",
		"five.example.com",
		"six.example.com",
	}

	// HRW can consistently select a uniformly-distributed set of servers for
	// any given key
	var (
		key = []byte("/examples/object-key")
		h   = Hash(key)
	)

	SortSliceByValue(servers, h)
	for id := range servers {
		fmt.Printf("trying GET %s%s\n", servers[id], key)
	}

	// Output:
	// trying GET four.example.com/examples/object-key
	// trying GET three.example.com/examples/object-key
	// trying GET one.example.com/examples/object-key
	// trying GET two.example.com/examples/object-key
	// trying GET six.example.com/examples/object-key
	// trying GET five.example.com/examples/object-key
}
func (h hashString) Hash() uint64 {
	return Hash([]byte(h))
}

func TestSortSliceByIndex(t *testing.T) {
	actual := []string{"a", "b", "c", "d", "e", "f"}
	expect := []string{"e", "a", "c", "f", "d", "b"}
	hash := Hash(testKey)
	SortSliceByIndex(actual, hash)
	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Was %#v, but expected %#v", actual, expect)
	}
}

func TestSortSliceByValue(t *testing.T) {
	actual := []string{"a", "b", "c", "d", "e", "f"}
	expect := []string{"d", "b", "a", "f", "c", "e"}
	hash := Hash(testKey)
	SortSliceByValue(actual, hash)
	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Was %#v, but expected %#v", actual, expect)
	}
}

func TestSortByRule(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		//                  0    1    2    3    4    5
		actual := []string{"a", "b", "c", "d", "e", "f"}
		//                  4    2    0    5    3    1
		expect := []string{"c", "f", "b", "e", "a", "d"}
		rule := []uint64{4, 2, 0, 5, 3, 1}

		sortByRuleDirect(
			func(i, j int) { actual[i], actual[j] = actual[j], actual[i] },
			6, rule)

		if !reflect.DeepEqual(actual, expect) {
			t.Errorf("Was %#v, but expected %#v", actual, expect)
		}
	})

	t.Run("inverse", func(t *testing.T) {
		//                  0    1    2    3    4    5
		actual := []string{"a", "b", "c", "d", "e", "f"}
		//                  4    2    0    5    3    1
		expect := []string{"e", "c", "a", "f", "d", "b"}
		rule := []uint64{4, 2, 0, 5, 3, 1}

		sortByRuleInverse(
			func(i, j int) { actual[i], actual[j] = actual[j], actual[i] },
			6, rule)

		if !reflect.DeepEqual(actual, expect) {
			t.Errorf("Was %#v, but expected %#v", actual, expect)
		}
	})
}

func TestSortSliceByValueFail(t *testing.T) {
	t.Run("empty slice", func(t *testing.T) {
		var (
			actual []int
			hash   = Hash(testKey)
		)
		SortSliceByValue(actual, hash)
	})

	t.Run("must be slice", func(t *testing.T) {
		actual := 10
		hash := Hash(testKey)
		SortSliceByValue(actual, hash)
	})

	t.Run("must 'fail' for unknown type", func(t *testing.T) {
		actual := []byte{1, 2, 3, 4, 5}
		expect := []byte{1, 2, 3, 4, 5}
		hash := Hash(testKey)
		SortSliceByValue(actual, hash)
		if !reflect.DeepEqual(actual, expect) {
			t.Errorf("Was %#v, but expected %#v", actual, expect)
		}
	})
}

func TestSortSliceByValueHasher(t *testing.T) {
	actual := []hashString{"a", "b", "c", "d", "e", "f"}
	expect := []hashString{"d", "b", "a", "f", "c", "e"}
	hash := Hash(testKey)
	SortSliceByValue(actual, hash)
	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Was %#v, but expected %#v", actual, expect)
	}
}

func TestSortSliceByValueIntSlice(t *testing.T) {
	actual := []int{0, 1, 2, 3, 4, 5}
	expect := []int{1, 5, 3, 0, 4, 2}
	hash := Hash(testKey)
	SortSliceByValue(actual, hash)
	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Was %#v, but expected %#v", actual, expect)
	}
}

func TestSortByWeight(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	hash := Hash(testKey)
	actual := SortByWeight(nodes, hash)
	expected := []uint64{3, 1, 4, 2, 0}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Was %#v, but expected %#v", actual, expected)
	}
}

func TestUniformDistribution(t *testing.T) {
	const (
		size    = 10
		keys    = 100000
		percent = 0.03
	)

	t.Run("sortByWeight", func(t *testing.T) {
		var (
			i      uint64
			nodes  [size]uint64
			counts = make(map[uint64]uint64, size)
			key    = make([]byte, 16)
		)

		for i = 0; i < size; i++ {
			nodes[i] = i
		}

		for i = 0; i < keys; i++ {
			binary.BigEndian.PutUint64(key, i)
			hash := Hash(key)
			counts[SortByWeight(nodes[:], hash)[0]]++
		}

		mean := float64(keys) / float64(size)
		delta := mean * percent
		for node, count := range counts {
			d := mean - float64(count)
			if d > delta || (0-d) > delta {
				t.Errorf(
					"Node %d received %d keys, expected %v (+/- %v)",
					node, count, mean, delta,
				)
			}
		}
	})

	t.Run("sortByIndex", func(t *testing.T) {
		var (
			i      uint64
			a, b   [size]uint64
			counts = make(map[uint64]int, size)
			key    = make([]byte, 16)
		)

		for i = 0; i < size; i++ {
			a[i] = i
		}

		for i = 0; i < keys; i++ {
			copy(b[:], a[:])

			binary.BigEndian.PutUint64(key, i)
			hash := Hash(key)
			SortSliceByIndex(b[:], hash)
			counts[b[0]]++
		}

		mean := float64(keys) / float64(size)
		delta := mean * percent
		for node, count := range counts {
			d := mean - float64(count)
			if d > delta || (0-d) > delta {
				t.Errorf(
					"Node %d received %d keys, expected %.0f (+/- %.2f)",
					node, count, mean, delta,
				)
			}
		}
	})

	t.Run("sortByValue", func(t *testing.T) {
		var (
			i      uint64
			a, b   [size]int
			counts = make(map[int]int, size)
			key    = make([]byte, 16)
		)

		for i = 0; i < size; i++ {
			a[i] = int(i)
		}

		for i = 0; i < keys; i++ {
			copy(b[:], a[:])
			binary.BigEndian.PutUint64(key, i)
			hash := Hash(key)
			SortSliceByValue(b[:], hash)
			counts[b[0]]++
		}

		mean := float64(keys) / float64(size)
		delta := mean * percent
		for node, count := range counts {
			d := mean - float64(count)
			if d > delta || (0-d) > delta {
				t.Errorf(
					"Node %d received %d keys, expected %.0f (+/- %.2f)",
					node, count, mean, delta,
				)
			}
		}
	})

	t.Run("hash collision", func(t *testing.T) {
		var (
			i      uint64
			counts = make(map[uint64]uint64)
			key    = make([]byte, 16)
		)

		for i = 0; i < keys; i++ {
			binary.BigEndian.PutUint64(key, i)
			hash := Hash(key)
			counts[hash]++
		}

		for node, count := range counts {
			if count > 1 {
				t.Errorf("Node %d received %d keys", node, count)
			}
		}
	})
}

func BenchmarkSortByWeight_fnv_10(b *testing.B) {
	hash := Hash(testKey)
	_ = benchmarkSortByWeight(b, 10, hash)
}

func BenchmarkSortByWeight_fnv_100(b *testing.B) {
	hash := Hash(testKey)
	_ = benchmarkSortByWeight(b, 100, hash)
}

func BenchmarkSortByWeight_fnv_1000(b *testing.B) {
	hash := Hash(testKey)
	_ = benchmarkSortByWeight(b, 1000, hash)
}

func BenchmarkSortByIndex_fnv_10(b *testing.B) {
	hash := Hash(testKey)
	benchmarkSortByIndex(b, 10, hash)
}

func BenchmarkSortByIndex_fnv_100(b *testing.B) {
	hash := Hash(testKey)
	benchmarkSortByIndex(b, 100, hash)
}

func BenchmarkSortByIndex_fnv_1000(b *testing.B) {
	hash := Hash(testKey)
	benchmarkSortByIndex(b, 1000, hash)
}

func BenchmarkSortByValue_fnv_10(b *testing.B) {
	hash := Hash(testKey)
	benchmarkSortByValue(b, 10, hash)
}

func BenchmarkSortByValue_fnv_100(b *testing.B) {
	hash := Hash(testKey)
	benchmarkSortByValue(b, 100, hash)
}

func BenchmarkSortByValue_fnv_1000(b *testing.B) {
	hash := Hash(testKey)
	benchmarkSortByValue(b, 1000, hash)
}

func benchmarkSortByWeight(b *testing.B, n int, hash uint64) uint64 {
	servers := make([]uint64, n)
	for i := uint64(0); i < uint64(len(servers)); i++ {
		servers[i] = i
	}

	b.ResetTimer()
	b.ReportAllocs()

	var x uint64
	for i := 0; i < b.N; i++ {
		x += SortByWeight(servers, hash)[0]
	}
	return x
}

func benchmarkSortByIndex(b *testing.B, n int, hash uint64) {
	servers := make([]uint64, n)
	for i := uint64(0); i < uint64(len(servers)); i++ {
		servers[i] = i
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		SortSliceByIndex(servers, hash)
	}
}

func benchmarkSortByValue(b *testing.B, n int, hash uint64) {
	servers := make([]string, n)
	for i := uint64(0); i < uint64(len(servers)); i++ {
		servers[i] = "localhost:" + strconv.FormatUint(60000-i, 10)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		SortSliceByValue(servers, hash)
	}
}

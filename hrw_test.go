package hrw

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"reflect"
	"strconv"
	"testing"

	"github.com/reusee/mmh3"
)

type hashString string

var testKey = []byte("Golang simple HRW implementation")

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
		h   = hash(key)
		err = SortSliceByValue(servers, h)
	)

	if err != nil {
		panic(err)
	}

	for id := range servers {
		fmt.Printf("trying GET %s%s\n", servers[id], key)
	}

	// Output:
	// trying GET six.example.com/examples/object-key
	// trying GET one.example.com/examples/object-key
	// trying GET three.example.com/examples/object-key
	// trying GET four.example.com/examples/object-key
	// trying GET five.example.com/examples/object-key
	// trying GET two.example.com/examples/object-key
}
func (h hashString) Hash() uint64 {
	hs := fnv.New64()
	// error always nil
	_, _ = hs.Write([]byte(h))
	return (hs.Sum64() >> 1) % m64
}

func hash(key []byte) uint64 {
	h := fnv.New64()
	// error always nil
	_, _ = h.Write(key)
	return (h.Sum64() >> 1) ^ m64
}

func mur3hash(key []byte) uint64 {
	h := mmh3.New128()
	// error always nil
	_, _ = h.Write(key)

	var (
		data   = h.Sum(nil)
		length = len(data)
		result uint64
	)

	for i := 0; i < length; i++ {
		result += uint64(data[i]) << uint64(length-i)
	}

	return result
}

func TestSortSliceByIndex(t *testing.T) {
	actual := []string{"a", "b", "c", "d", "e", "f"}
	expect := []string{"e", "a", "c", "d", "b", "f"}

	hash := hash(testKey)

	SortSliceByIndex(actual, hash)
	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Was %#v, but expected %#v", actual, expect)
	}
}

func TestSortSliceByValue(t *testing.T) {
	actual := []string{"a", "b", "c", "d", "e", "f"}
	expect := []string{"e", "b", "c", "d", "f", "a"}

	hash := hash(testKey)

	if err := SortSliceByValue(actual, hash); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Was %#v, but expected %#v", actual, expect)
	}
}

func TestSortSliceByValueFail(t *testing.T) {
	t.Run("empty slice", func(t *testing.T) {
		actual := make([]int, 0)
		hash := hash(testKey)

		if err := SortSliceByValue(actual, hash); err != nil {
			t.Fatal(err)
		}

	})

	t.Run("must be slice", func(t *testing.T) {
		actual := 10
		hash := hash(testKey)

		if err := SortSliceByValue(actual, hash); err == nil {
			t.Fatal("must fail for bad type")
		}

	})

	t.Run("must fail for unknown type", func(t *testing.T) {
		actual := []byte{1, 2, 3, 4, 5}
		hash := hash(testKey)

		if err := SortSliceByValue(actual, hash); err == nil {
			t.Fatal("must fail for bad type")
		}
	})
}

func TestSortSliceByValueHasher(t *testing.T) {
	actual := []hashString{"a", "b", "c", "d", "e", "f"}
	expect := []hashString{"e", "d", "c", "a", "b", "f"}

	hash := hash(testKey)

	if err := SortSliceByValue(actual, hash); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Was %#v, but expected %#v", actual, expect)
	}
}

func TestSortSliceByValueIntSlice(t *testing.T) {
	actual := []int{0, 1, 2, 3, 4, 5}
	expect := []int{2, 0, 5, 3, 4, 1}

	hash := hash(testKey)

	if err := SortSliceByValue(actual, hash); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Was %#v, but expected %#v", actual, expect)
	}
}

func TestSortByWeight(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	hash := mur3hash(testKey)

	actual := SortByWeight(nodes, hash)
	expected := []uint64{0, 1, 4, 2, 3}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Was %#v, but expected %#v", actual, expected)
	}
}

func TestUniformDistribution(t *testing.T) {
	var (
		i      uint64
		size   = uint64(4)
		nodes  = make([]uint64, 0, size)
		counts = make(map[uint64]uint64)
		key    = make([]byte, 16)
		keys   = uint64(10000000)
	)

	for i = 0; i < size; i++ {
		nodes = append(nodes, i)
	}

	for i = 0; i < keys; i++ {
		binary.BigEndian.PutUint64(key, i)
		hash := hash(key)
		counts[SortByWeight(nodes, hash)[0]]++
	}

	mean := float64(keys) / float64(len(nodes))
	delta := mean * 0.002 // 0.2%
	for node, count := range counts {
		d := mean - float64(count)
		if d > delta || (0-d) > delta {
			t.Errorf(
				"Node %d received %d keys, expected %v (+/- %v)",
				node, count, mean, delta,
			)
		}
	}
}

func BenchmarkSortByWeight_fnv_10(b *testing.B) {
	hash := hash(testKey)
	_ = benchmarkSortByWeight(b, 10, hash)
}

func BenchmarkSortByWeight_fnv_100(b *testing.B) {
	hash := hash(testKey)
	_ = benchmarkSortByWeight(b, 100, hash)
}

func BenchmarkSortByWeight_fnv_1000(b *testing.B) {
	hash := hash(testKey)
	_ = benchmarkSortByWeight(b, 1000, hash)
}

func BenchmarkSortByIndex_fnv_10(b *testing.B) {
	hash := hash(testKey)
	benchmarkSortByIndex(b, 10, hash)
}

func BenchmarkSortByIndex_fnv_100(b *testing.B) {
	hash := hash(testKey)
	benchmarkSortByIndex(b, 100, hash)
}

func BenchmarkSortByIndex_fnv_1000(b *testing.B) {
	hash := hash(testKey)
	benchmarkSortByIndex(b, 1000, hash)
}

func BenchmarkSortByValue_fnv_10(b *testing.B) {
	hash := hash(testKey)
	benchmarkSortByValue(b, 10, hash)
}

func BenchmarkSortByValue_fnv_100(b *testing.B) {
	hash := hash(testKey)
	benchmarkSortByValue(b, 100, hash)
}

func BenchmarkSortByValue_fnv_1000(b *testing.B) {
	hash := hash(testKey)
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
		if err := SortSliceByValue(servers, hash); err != nil {
			b.Fatal(err)
		}
	}
}

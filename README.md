# Golang HRW implementation

[![Build Status](https://travis-ci.org/im-kulikov/hrw.svg?branch=master)](https://travis-ci.org/im-kulikov/hrw)
[![codecov](https://codecov.io/gh/im-kulikov/hrw/badge.svg)](https://codecov.io/gh/im-kulikov/hrw)
[![Report](https://goreportcard.com/badge/github.com/im-kulikov/hrw)](https://goreportcard.com/report/github.com/im-kulikov/hrw)
[![GitHub release](https://img.shields.io/github/release/im-kulikov/hrw.svg)](https://github.com/im-kulikov/hrw)

[Rendezvous or highest random weight](https://en.wikipedia.org/wiki/Rendezvous_hashing) (HRW) hashing is an algorithm that allows clients to achieve distributed agreement on a set of k options out of a possible set of n options. A typical application is when clients need to agree on which sites (or proxies) objects are assigned to. When k is 1, it subsumes the goals of consistent hashing, using an entirely different method.

## Install

`go get github.com/im-kulikov/hrw`

## Example

```go
package main

import (
	"fmt"
	
	"github.com/im-kulikov/hrw"
)

func main() {
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
		h   = hrw.Hash(key)
	)

	hrw.SortSliceByValue(servers, h)
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
```
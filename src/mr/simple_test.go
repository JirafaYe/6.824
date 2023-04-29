package mr

import (
	"fmt"
	"testing"
)

func TestJob(t *testing.T) {
	c := make(map[int]Job, 10)
	fmt.Println("hello")
	c[0] = Job{}

	for k, v := range c {
		fmt.Println(k, v)
	}
}

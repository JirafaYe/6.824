package mr

import (
	"fmt"
	"testing"
)

func TestJob(t *testing.T) {
	i := ihash("10")
	fmt.Print(i)
}

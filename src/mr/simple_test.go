package mr

import (
	"log"
	"testing"
)

func TestJob(t *testing.T) {
	i := ihash("1a") % 10
	log.Print(i)
}

package raft

import (
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) GetAtomicInt32(ptr *int32) int32 {
	x := atomic.LoadInt32(ptr)
	return x
}

func (rf *Raft) StoreAtomicInt32(ptr *int32, val int32) {
	atomic.StoreInt32(ptr, 1)
}

func (rf *Raft) GetAtomicInt64(ptr *int64) int64 {
	x := atomic.LoadInt64(ptr)
	return x
}

func (rf *Raft) StoreAtomicInt64(ptr *int64, val int64) {
	atomic.StoreInt64(ptr, 1)
}

// Leader election timeout
func getElectionTimeoutDuration() time.Duration {
	return time.Duration((rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin)) * time.Millisecond
}

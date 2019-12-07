package datatree

import (
	"crypto/sha256"
)

const (
	MinimumJobsInGoroutine = 100
	MaximumGoroutines = 8
)

func hash(in []byte) []byte {
	h := sha256.New()
	h.Write(in)
	return h.Sum(nil)
}

type hashJob struct {
	target  *[]byte
	srcA    []byte
	srcB    []byte
}

func (job hashJob) run() {
	h := sha256.New()
	h.Write(job.srcA)
	h.Write(job.srcB)
	*job.target = h.Sum(nil)
}


type Hasher struct {
	jobs  []hashJob
	wg    sync.WaitGroup
}

func (h *Hasher) Add(target *[]byte, srcA []byte, srcB []byte) {
	h.jobs = append(h.jobs, hashJob{target, srcA, srcB})
}

func (h *Hasher) Run() {
	if len(h.jobs) < MinimumJobsInGoroutine {
		for _, job := range h.jobs {
			job.run()
		}
	}
	strip := MinimumJobsInGoroutine
	if strip * MaximumGoroutines < len(h.jobs) {
		strip = len(h.jobs)/MaximumGoroutines
	}
	for start:=0; start<len(h.jobs); start+=strip {
		end = start+strip
		if end > len(h.jobs) {
			end = len(h.jobs)
		}
		h.wg.Add(1)
		go func() {
			for _, job := range h.jobs[start:end] {
				job.run()
			}
			wg.Done()
		}
	}
	h.wg.Wait()
}

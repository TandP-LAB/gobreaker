package gobreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testClock struct{ nanos atomic.Int64 }

func newTestClock() *testClock {
	c := new(testClock)
	c.nanos.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	return c
}

func (c *testClock) now() time.Time          { return time.Unix(0, c.nanos.Load()) }
func (c *testClock) advance(d time.Duration) { c.nanos.Add(int64(d)) }

func TestAdmissionStateUsesNativeClockAndGeneration(t *testing.T) {
	clock := newTestClock()
	cb := NewTwoStepCircuitBreaker[any](Settings{
		Now: clock.now, Timeout: time.Second,
		ReadyToTrip: func(c Counts) bool { return c.TotalFailures >= 1 },
	})
	state, stale, err := cb.AllowWithState()
	if err != nil || state != StateClosed || stale == nil {
		t.Fatalf("closed admission: %v %v", state, err)
	}
	_, trip, err := cb.AllowWithState()
	if err != nil {
		t.Fatal(err)
	}
	trip(errors.New("failed"))
	state, done, err := cb.AllowWithState()
	if !errors.Is(err, ErrOpenState) || state != StateOpen || done != nil {
		t.Fatalf("open refusal: %v %v", state, err)
	}
	clock.advance(time.Second + time.Nanosecond)
	state, probe, err := cb.AllowWithState()
	if err != nil || state != StateHalfOpen || probe == nil {
		t.Fatalf("half-open admission: %v %v", state, err)
	}
	state, done, err = cb.AllowWithState()
	if !errors.Is(err, ErrTooManyRequests) || state != StateHalfOpen || done != nil {
		t.Fatalf("half-open refusal: %v %v", state, err)
	}
	stale(errors.New("late old generation"))
	if cb.Counts().TotalFailures != 0 {
		t.Fatal("old completion mutated the probe generation")
	}
	probe(nil)
	state, done, err = cb.AllowWithState()
	if err != nil || state != StateClosed {
		t.Fatalf("probe recovery: %v %v", state, err)
	}
	done(nil)
}

func TestAdmissionStateConcurrentHalfOpenAndCompletion(t *testing.T) {
	clock := newTestClock()
	cb := NewTwoStepCircuitBreaker[any](Settings{Now: clock.now, Timeout: time.Second,
		ReadyToTrip: func(c Counts) bool { return c.TotalFailures >= 1 }})
	_, trip, _ := cb.AllowWithState()
	trip(errors.New("failed"))
	clock.advance(2 * time.Second)
	const callers = 32
	results := make(chan func(error), callers)
	errorsFound := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			state, done, err := cb.AllowWithState()
			if state != StateHalfOpen || (err != nil && !errors.Is(err, ErrTooManyRequests)) {
				errorsFound <- errors.New("invalid half-open outcome")
			}
			if err == nil {
				results <- done
			} else if done != nil {
				errorsFound <- errors.New("refused request returned completion")
			}
		}()
	}
	wg.Wait()
	close(results)
	if len(errorsFound) != 0 || len(results) != 1 {
		t.Fatalf("admissions=%d errors=%d", len(results), len(errorsFound))
	}
	probe := <-results
	probe(nil)
	_, done, err := cb.AllowWithState()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); done(nil) }()
	}
	wg.Wait()
	counts := cb.Counts()
	if counts.Requests != 1 || counts.TotalSuccesses != 1 {
		t.Fatalf("duplicate completion counted: %+v", counts)
	}
}

func TestAdmissionStateCompletionKeepsWindowAge(t *testing.T) {
	clock := newTestClock()
	cb := NewTwoStepCircuitBreaker[any](Settings{Now: clock.now, Interval: 2 * time.Second, BucketPeriod: time.Second})
	_, old, err := cb.AllowWithState()
	if err != nil {
		t.Fatal(err)
	}
	clock.advance(3 * time.Second)
	_, current, err := cb.AllowWithState()
	if err != nil {
		t.Fatal(err)
	}
	old(errors.New("expired bucket"))
	if cb.Counts().TotalFailures != 0 {
		t.Fatal("old window completion affected the current window")
	}
	current(nil)
	if cb.Counts().TotalSuccesses != 1 {
		t.Fatal("current completion was not counted")
	}
}

func TestAdmissionStateDefaultClockAndExistingPaths(t *testing.T) {
	cb := NewTwoStepCircuitBreaker[any](Settings{})
	done, err := cb.Allow()
	if err != nil || done == nil {
		t.Fatalf("default clock admission: %v", err)
	}
	done(nil)
	clock := newTestClock()
	oneStep := NewCircuitBreaker[int](Settings{Now: clock.now})
	got, err := oneStep.Execute(func() (int, error) { return 7, nil })
	if err != nil || got != 7 || oneStep.Counts().TotalSuccesses != 1 {
		t.Fatalf("execute: %v %v", got, err)
	}
}

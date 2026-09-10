/*
Copyright 2026 The InftyAI Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package provider is the port logship reads instance logs through: one implementation per Nebula
// provider, Modal being the first.
//
// Which one a stream comes from is decided per Pod, from the provider nodeSelector Nebula placed it
// with, and is deliberately not configured anywhere. A configured provider would have to be kept in
// agreement with the cluster's node pools, and being wrong about it looks exactly like a quiet
// cluster: every Pod skipped, nothing shipped, no error. Routing per Pod also means a cluster split
// across two providers ships from both, with one process and no extra wiring.
package provider

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/InftyAI/Nebula/components/logship/internal/ship"
)

// The provider names, which are the values Nebula's placement controller writes to a Pod's provider
// nodeSelector. A name is what a backend is registered under, so one that disagrees with the
// controller's makes every Pod on that provider unshippable.
const (
	ProviderModal = "modal"
)

// Provider reads one backend's instance logs. Implementations live in their own package and are
// registered on a Set; nothing outside this port may name one.
//
// Every method may be called concurrently, and for many instances at once: one process follows the
// whole cluster's streams.
type Provider interface {
	// Streams are the streams each of this backend's instances has, in the order they are worth
	// reading, named in whatever vocabulary the backend uses. One pipeline runs per name per
	// instance, and nothing between here and Source interprets a name — it only has to round-trip.
	Streams() []string

	// Source binds one instance's stream so it can be followed from a cursor.
	//
	// It must fail rather than return a Source that ships nothing: an instance id or stream name the
	// backend does not recognise is a wiring mistake, and the symptom of a silent one is a run whose
	// logs are simply absent.
	Source(instanceID, stream string) (ship.Source, error)

	// Reserve makes room for instances instances BEFORE any of their streams open, for a backend
	// whose transport caps concurrent streams per connection. A backend with no such cap does
	// nothing here.
	//
	// instances is an upper bound, not this backend's exact share: a caller that cannot cheaply
	// attribute the fleet per backend passes the whole fleet's size, so over-reserving has to be
	// cheap and must never be an error.
	Reserve(instances int) error

	// Close releases the backend's connections, once, at shutdown.
	Close() error
}

// Factory opens one backend. Called at most once per process and only when a Pod needs that
// backend: opening reads credentials and dials, and a cluster with nothing on a provider must not
// need either to be present.
type Factory func() (Provider, error)

// Set is this process's providers — what is registered, and what has been opened.
//
// A backend's name lives in its registration rather than on the Provider itself, so the two cannot
// disagree about the value a Pod's nodeSelector has to match. Safe for concurrent use.
type Set struct {
	mu      sync.Mutex
	factory map[string]Factory
	open    map[string]Provider
}

func NewSet() *Set {
	return &Set{factory: map[string]Factory{}, open: map[string]Provider{}}
}

// Register adds a backend under the name a Pod's provider nodeSelector carries.
//
// Panics on a duplicate or an empty name, because both are programmer errors in this process's own
// wiring: picking one of two factories at runtime would hide which backend is actually reading.
func (s *Set) Register(name string, open Factory) {
	if name == "" || open == nil {
		panic("provider: Register needs a name and a factory")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.factory[name]; dup {
		panic(fmt.Sprintf("provider: duplicate registration for %q", name))
	}
	s.factory[name] = open
}

// Get returns the backend for name, opening it on first use.
//
// An unregistered name is an error and never a nil Provider: it means the cluster placed a Pod on a
// backend this build cannot read, which is a real state — a provider added to Nebula before it is
// added here — and its only other symptom is logs that never arrive.
//
// A factory that fails is retried on the next call rather than remembered. The cause is nearly always
// a missing credential, so the retry is unlikely to succeed, and it is the repetition that gets the
// misconfiguration into the log where someone sees it.
func (s *Set) Get(name string) (Provider, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if p := s.open[name]; p != nil {
		return p, nil
	}
	open := s.factory[name]
	if open == nil {
		return nil, fmt.Errorf("no log provider for %q; this build has %v", name, s.names())
	}
	p, err := open()
	if err != nil {
		return nil, fmt.Errorf("opening the %s log provider: %w", name, err)
	}
	s.open[name] = p
	return p, nil
}

// Names are the registered names, sorted, for an error that says what this build does support.
func (s *Set) Names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.names()
}

// Close closes every backend that was opened, leaving the registrations. Errors are joined: one
// backend failing to close says nothing about the others, and all of them are worth reporting.
func (s *Set) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	errs := make([]error, 0, len(s.open))
	for name, p := range s.open {
		if err := p.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing the %s log provider: %w", name, err))
		}
	}
	s.open = map[string]Provider{}
	return errors.Join(errs...)
}

func (s *Set) names() []string {
	names := make([]string, 0, len(s.factory))
	for name := range s.factory {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

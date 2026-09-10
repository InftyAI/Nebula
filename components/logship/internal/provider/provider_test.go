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

package provider

import (
	"fmt"
	"strings"
	"testing"

	"github.com/InftyAI/Nebula/components/logship/internal/ship"
)

type stub struct {
	opened int
	closed int
}

func (s *stub) Streams() []string { return []string{"stdout"} }
func (s *stub) Source(string, string) (ship.Source, error) {
	return nil, fmt.Errorf("not used here")
}
func (s *stub) Reserve(int) error { return nil }
func (s *stub) Close() error      { s.closed++; return nil }

func (s *stub) factory() Factory {
	return func() (Provider, error) {
		s.opened++
		return s, nil
	}
}

func TestGetOpensOnceAndClosesWhatItOpened(t *testing.T) {
	// Opening once per name is what makes a backend's connection pool the whole process's, rather
	// than one pool per instance the watch happens to deliver.
	p := &stub{}
	s := NewSet()
	s.Register(ProviderModal, p.factory())

	for range 3 {
		if _, err := s.Get(ProviderModal); err != nil {
			t.Fatal(err)
		}
	}
	if p.opened != 1 {
		t.Errorf("opened %d times, want 1", p.opened)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if p.closed != 1 {
		t.Errorf("closed %d times, want 1", p.closed)
	}
}

func TestNothingIsOpenedUntilAPodNeedsIt(t *testing.T) {
	// The reason this is lazy: a cluster with nothing on a provider must not need its credentials to
	// be present, so the process cannot open every registered backend at startup.
	p := &stub{}
	s := NewSet()
	s.Register(ProviderModal, p.factory())

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if p.opened != 0 || p.closed != 0 {
		t.Errorf("opened %d, closed %d; want a registration to touch neither", p.opened, p.closed)
	}
}

func TestGetOnAnUnregisteredProviderSaysWhatIsSupported(t *testing.T) {
	// Nebula gaining a provider before logship does is a real state, and the Pods it places are then
	// unreadable here. The error is the only thing that tells them apart from an idle cluster.
	s := NewSet()
	s.Register(ProviderModal, (&stub{}).factory())

	_, err := s.Get("aws")
	if err == nil {
		t.Fatal("Get accepted a provider with no backend")
	}
	if !strings.Contains(err.Error(), ProviderModal) {
		t.Errorf("error %q does not name the providers this build has", err)
	}
}

func TestAFailedOpenIsRetried(t *testing.T) {
	// Not remembered: the repetition in the log is what surfaces a missing credential, and a cached
	// failure would report it once, at a moment nobody was looking.
	calls := 0
	s := NewSet()
	s.Register(ProviderModal, func() (Provider, error) {
		calls++
		return nil, fmt.Errorf("MODAL_TOKEN_ID is not set")
	})

	for range 2 {
		if _, err := s.Get(ProviderModal); err == nil {
			t.Fatal("Get hid a factory failure")
		}
	}
	if calls != 2 {
		t.Errorf("factory called %d times, want one per Get", calls)
	}
}

func TestRegisterRejectsADuplicate(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("a second registration for one name did not panic")
		}
	}()
	s := NewSet()
	s.Register(ProviderModal, (&stub{}).factory())
	s.Register(ProviderModal, (&stub{}).factory())
}

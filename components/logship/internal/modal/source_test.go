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

package modal

import (
	"testing"

	pb "github.com/modal-labs/modal-client/go/proto/modal_proto"
)

func TestDescriptorRoundTrips(t *testing.T) {
	// The two directions are used at opposite ends of the wiring — a name goes into a stream name and
	// comes back out to open an RPC — so a rename that touches only one of them loses a whole stream.
	for _, fd := range []pb.FileDescriptor{
		pb.FileDescriptor_FILE_DESCRIPTOR_STDOUT,
		pb.FileDescriptor_FILE_DESCRIPTOR_STDERR,
	} {
		got, err := ParseDescriptor(Descriptor(fd))
		if err != nil {
			t.Fatalf("ParseDescriptor(Descriptor(%v)): %v", fd, err)
		}
		if got != fd {
			t.Fatalf("round trip of %v gave %v", fd, got)
		}
	}
}

func TestParseDescriptorRejectsAnUnknownName(t *testing.T) {
	// Returning UNSPECIFIED with no error would open a stream that ships nothing at all.
	if _, err := ParseDescriptor("stdlog"); err == nil {
		t.Fatal("ParseDescriptor accepted a name that maps to no descriptor")
	}
	if _, err := ParseDescriptor(Descriptor(pb.FileDescriptor_FILE_DESCRIPTOR_UNSPECIFIED)); err == nil {
		t.Fatal(`ParseDescriptor accepted "unknown", which Descriptor uses as a fallback name`)
	}
}

func TestDescriptorsAreTheStreamsAPipelineIsBuiltFor(t *testing.T) {
	if len(Descriptors) != 2 {
		t.Fatalf("Descriptors = %v, want the two real descriptors", Descriptors)
	}
	for _, name := range Descriptors {
		if _, err := ParseDescriptor(name); err != nil {
			t.Fatalf("Descriptors contains %q, which ParseDescriptor rejects: %v", name, err)
		}
	}
}

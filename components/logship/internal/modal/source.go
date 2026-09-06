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
	"context"
	"fmt"

	pb "github.com/modal-labs/modal-client/go/proto/modal_proto"

	"github.com/InftyAI/Nebula/components/logship/internal/ship"
)

// Source binds one sandbox and one file descriptor to a ship.Source. The dependency runs this way
// round on purpose: ship declares the port and knows nothing about Modal.
//
// One per stream, because a cursor is only meaningful for one descriptor of one sandbox.
type Source struct {
	Client  LogsClient
	Sandbox string
	FD      pb.FileDescriptor
}

// Follow implements ship.Source. It drops the per-entry FD: the stream was opened for one, so every
// entry in it has the same one, and repeating it per line would be the only thing ship had to know
// about Modal's proto.
func (s Source) Follow(ctx context.Context, cursor string, fn func(ship.Batch) error) error {
	return Follow(ctx, s.Client, s.Sandbox, s.FD, cursor, func(b Batch) error {
		entries := make([]ship.Entry, 0, len(b.Entries))
		for _, e := range b.Entries {
			entries = append(entries, ship.Entry{Data: e.Data, At: e.At})
		}
		return fn(ship.Batch{Entries: entries, Cursor: b.Cursor})
	})
}

// Descriptor names an FD for a CloudWatch stream name. Anything but the two real descriptors is
// "unknown" rather than an error: a stream that ships under an odd name is recoverable, and a
// sandbox whose logs are dropped over an enum value is not.
func Descriptor(fd pb.FileDescriptor) string {
	switch fd {
	case pb.FileDescriptor_FILE_DESCRIPTOR_STDOUT:
		return "stdout"
	case pb.FileDescriptor_FILE_DESCRIPTOR_STDERR:
		return "stderr"
	default:
		return "unknown"
	}
}

// Descriptors are the streams a sandbox has, in the order they are worth reading. The supervisor
// names streams with plain strings so it need not know Modal exists, so this is what it is given.
var Descriptors = []string{
	Descriptor(pb.FileDescriptor_FILE_DESCRIPTOR_STDOUT),
	Descriptor(pb.FileDescriptor_FILE_DESCRIPTOR_STDERR),
}

// ParseDescriptor is Descriptor's inverse, and unlike it this one errors: a name that maps to no
// descriptor would otherwise open a stream on FILE_DESCRIPTOR_UNSPECIFIED and ship nothing, silently.
func ParseDescriptor(name string) (pb.FileDescriptor, error) {
	for _, fd := range []pb.FileDescriptor{
		pb.FileDescriptor_FILE_DESCRIPTOR_STDOUT,
		pb.FileDescriptor_FILE_DESCRIPTOR_STDERR,
	} {
		if Descriptor(fd) == name {
			return fd, nil
		}
	}
	return pb.FileDescriptor_FILE_DESCRIPTOR_UNSPECIFIED, fmt.Errorf("unknown descriptor %q", name)
}

package velocitylog

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MarshalEntry marshals any proto message into a byte slice and panics if there is an error.
func MarshalEntry(pb proto.Message) []byte {
	// marshal the entity
	marshalEntry, err := proto.Marshal(pb)
	if err != nil {
		panic(fmt.Sprintf("error marshalling the entity: %v", err))
	}
	return marshalEntry
}

// UnmarshalEntry unmarshals the data into the entry and panics if there is an error.
func UnmarshalEntry(bytes []byte, entry protoreflect.ProtoMessage) {
	if err := proto.Unmarshal(bytes, entry); err != nil {
		panic(fmt.Sprintf("error unmarshalling the data: %v", err))
	}
}

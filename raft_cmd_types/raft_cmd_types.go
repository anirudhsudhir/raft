// Define the types to be used in the Raft commands which are committed to the state machine
// These types are seriliased and persisted to the disk as they are a part of the Raft log
// Include the defined types in the `TypesToSerialise` slice
// These types are registered as serializable types automatically by the Raft library

package raft_cmd_types

// This type is used by the distributed key-value store
type OpCommand string

// This type is used by the distributed key-value store
const (
	PutKey    OpCommand = "PutKey"
	AppendKey OpCommand = "AppendKey"
	GetKey    OpCommand = "GetKey"
	DeleteKey OpCommand = "DeleteKey"
)

// This type is used by the distributed key-value store
type Op struct {
	Cmd   OpCommand
	Key   string
	Value string
}

// Include the custom defined type in this slice
var TypeRegistry = []any{
	OpCommand(""),
	Op{},
}

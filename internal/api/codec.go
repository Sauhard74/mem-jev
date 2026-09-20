package api

import (
	"errors"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var errInvalidRequestBody = errors.New("invalid request body")

type strictJSONCodec struct{ name string }

func (c strictJSONCodec) Name() string { return c.name }

func (strictJSONCodec) Marshal(message any) ([]byte, error) {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return nil, errors.New("message is not protobuf")
	}
	return protojson.MarshalOptions{}.Marshal(protoMessage)
}

func (strictJSONCodec) Unmarshal(data []byte, message any) error {
	protoMessage, ok := message.(proto.Message)
	if !ok || len(data) == 0 {
		return errInvalidRequestBody
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, protoMessage); err != nil {
		return errInvalidRequestBody
	}
	return nil
}

type strictProtoCodec struct{}

func (strictProtoCodec) Name() string { return "proto" }

func (strictProtoCodec) Marshal(message any) ([]byte, error) {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return nil, errors.New("message is not protobuf")
	}
	return proto.Marshal(protoMessage)
}

func (strictProtoCodec) Unmarshal(data []byte, message any) error {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return errInvalidRequestBody
	}
	if err := proto.Unmarshal(data, protoMessage); err != nil || hasUnknownFields(protoMessage.ProtoReflect()) {
		return errInvalidRequestBody
	}
	return nil
}

func hasUnknownFields(message protoreflect.Message) bool {
	if len(message.GetUnknown()) != 0 {
		return true
	}
	found := false
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.IsMap() {
			if field.MapValue().Kind() != protoreflect.MessageKind && field.MapValue().Kind() != protoreflect.GroupKind {
				return true
			}
			value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
				found = hasUnknownFields(item.Message())
				return !found
			})
			return !found
		}
		if field.IsList() {
			if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
				return true
			}
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if hasUnknownFields(list.Get(index).Message()) {
					found = true
					return false
				}
			}
			return true
		}
		if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
			found = hasUnknownFields(value.Message())
			return !found
		}
		return true
	})
	return found
}

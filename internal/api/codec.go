package api

import (
	"errors"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
	if err := proto.Unmarshal(data, protoMessage); err != nil || len(protoMessage.ProtoReflect().GetUnknown()) != 0 {
		return errInvalidRequestBody
	}
	return nil
}

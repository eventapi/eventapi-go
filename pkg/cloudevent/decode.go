package cloudevent

import (
	"errors"

	"github.com/eventapi/eventapi-go/pkg/codec"
)

// DecodeAndWrap 反序列化事件数据并包装为带类型的 CloudEvent
func DecodeAndWrap[T any](
	data []byte,
	codec codec.Codec,
	metadata CloudEventMetadata,
) (*CloudEvent[*T], error) {
	var event T
	if err := codec.Decode(data, &event); err != nil {
		return nil, errors.Join(errors.New("decode failed"), err)
	}
	return &CloudEvent[*T]{
		CloudEventMetadata: metadata,
		Data:               &event,
	}, nil
}

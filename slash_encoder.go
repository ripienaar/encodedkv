package encodedkv

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
)

// SlashCodec turns keys like /this/is/a.test.key into a Base64 encoded values split on `/`
// it also ensures that wildcard watchers like /this/is will find all the keys below that point
// this is probably a bit naive, mainly just to show the idea
//
//    	b, err := js.CreateKeyValue(&nats.KeyValueConfig{
//			Bucket:  "TEST",
//			History: 5,
//	  	})
//
//		bucket := NewEncodedKV(b, &SlashCodec{})
//		// now just use bucket like always
type SlashCodec struct{}

func (e *SlashCodec) EncodeRange(keys string) (string, error) {
	ek, err := e.EncodeKey(keys)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s.>", ek), nil
}

func (*SlashCodec) EncodeKey(key string) (string, error) {
	res := []string{}
	for _, part := range strings.Split(strings.TrimPrefix(key, "/"), "/") {
		if part == ">" || part == "*" {
			res = append(res, part)
			continue
		}

		dst := make([]byte, base64.StdEncoding.EncodedLen(len(part)))
		base64.StdEncoding.Encode(dst, []byte(part))
		res = append(res, string(dst))
	}

	if len(res) == 0 {
		return "", nats.ErrInvalidKey
	}

	return strings.Join(res, "."), nil
}

func (*SlashCodec) DecodeKey(key string) (string, error) {
	res := []string{}
	for _, part := range strings.Split(key, ".") {
		k, err := base64.StdEncoding.DecodeString(part)
		if err != nil {
			return "", err
		}

		res = append(res, string(k))
	}

	if len(res) == 0 {
		return "", nats.ErrInvalidKey
	}

	return fmt.Sprintf("/%s", strings.Join(res, "/")), nil
}

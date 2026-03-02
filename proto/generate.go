package proto

//go:generate sh -c "nix shell nixpkgs#protobuf_33 --command protoc --plugin=protoc-gen-go=$(go tool -n protoc-gen-go) --go_out=.. --go_opt=module=github.com/anupcshan/cloudblockdev --go_opt=default_api_level=API_OPAQUE blob.proto"

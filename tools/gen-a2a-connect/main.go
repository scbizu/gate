// Command gen-a2a-connect generates Connect v2 bindings from the official A2A
// SDK descriptor. It runs from the repository root via make proto-generate.
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func main() {
	if err := generate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func generate() error {
	descriptor := a2apb.File_a2av1_proto
	request := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{descriptor.Path()},
		// The wire descriptor retains the upstream go_package option. Map it
		// to the versioned SDK package that Gate actually imports.
		Parameter: proto.String("paths=source_relative,Ma2av1.proto=github.com/a2aproject/a2a-go/v2/a2apb/v1;a2apb"),
		ProtoFile: dependencies(descriptor),
	}
	input, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	// The generator version comes from go.mod, just like the SDK descriptor.
	command := exec.Command("go", "run", "connectrpc.com/connect/v2/cmd/protoc-gen-connect-go")
	command.Stdin = bytes.NewReader(input)
	command.Stderr = os.Stderr
	output, err := command.Output()
	if err != nil {
		return fmt.Errorf("run Connect generator: %w", err)
	}
	var response pluginpb.CodeGeneratorResponse
	if err := proto.Unmarshal(output, &response); err != nil {
		return fmt.Errorf("decode generator response: %w", err)
	}
	if response.GetError() != "" {
		return fmt.Errorf("Connect generator: %s", response.GetError())
	}
	if len(response.File) == 0 {
		return fmt.Errorf("Connect generator produced no bindings")
	}
	for _, file := range response.File {
		if !filepath.IsLocal(file.GetName()) {
			return fmt.Errorf("invalid generated path %q", file.GetName())
		}
		name := filepath.Join("gen", "a2a", filepath.FromSlash(file.GetName()))
		if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(name, []byte(file.GetContent()), 0644); err != nil {
			return err
		}
	}
	return nil
}

// Protoc plugins require imported descriptors before the files that use them.
func dependencies(root protoreflect.FileDescriptor) []*descriptorpb.FileDescriptorProto {
	seen := make(map[string]bool)
	var result []*descriptorpb.FileDescriptorProto
	var visit func(protoreflect.FileDescriptor)
	visit = func(file protoreflect.FileDescriptor) {
		if seen[file.Path()] {
			return
		}
		seen[file.Path()] = true
		imports := file.Imports()
		for i := 0; i < imports.Len(); i++ {
			visit(imports.Get(i).FileDescriptor)
		}
		result = append(result, protodesc.ToFileDescriptorProto(file))
	}
	visit(root)
	return result
}

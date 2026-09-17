package proto_test

import (
	"os"
	"testing"

	a2av1 "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestExampleAgentCardMatchesSchema(t *testing.T) {
	b, err := os.ReadFile("examples/research-engineering-agent-card.json")
	if err != nil {
		t.Fatal(err)
	}
	var card a2av1.AgentCard
	if err := protojson.Unmarshal(b, &card); err != nil {
		t.Fatalf("unmarshal example Agent Card: %v", err)
	}
	if got := len(card.GetCapabilities().GetExtensions()); got != 3 {
		t.Fatalf("extension count = %d, want 3", got)
	}
}

func TestCanonicalServiceStreamingShape(t *testing.T) {
	if got := a2av1.File_a2av1_proto.Path(); got != "a2av1.proto" {
		t.Fatalf("A2A descriptor path = %q, want canonical path %q", got, "a2av1.proto")
	}
	service := a2av1.File_a2av1_proto.Services().ByName("A2AService")
	if service == nil {
		t.Fatal("A2AService descriptor is missing")
	}

	wantServerStreaming := map[string]bool{
		"SendMessage":                      false,
		"SendStreamingMessage":             true,
		"GetTask":                          false,
		"ListTasks":                        false,
		"CancelTask":                       false,
		"SubscribeToTask":                  true,
		"CreateTaskPushNotificationConfig": false,
		"GetTaskPushNotificationConfig":    false,
		"ListTaskPushNotificationConfigs":  false,
		"GetExtendedAgentCard":             false,
		"DeleteTaskPushNotificationConfig": false,
	}
	if got := service.Methods().Len(); got != len(wantServerStreaming) {
		t.Fatalf("A2AService method count = %d, want %d", got, len(wantServerStreaming))
	}
	for name, want := range wantServerStreaming {
		method := service.Methods().ByName(protoreflect.Name(name))
		if method == nil {
			t.Errorf("A2AService.%s is missing", name)
			continue
		}
		if got := method.IsStreamingServer(); got != want {
			t.Errorf("A2AService.%s server-streaming = %t, want %t", name, got, want)
		}
		if method.IsStreamingClient() {
			t.Errorf("A2AService.%s unexpectedly uses client streaming", name)
		}
	}
}

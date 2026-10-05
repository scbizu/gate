package e2e_test

import (
	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	"os"
	"testing"
)

func messageRequest(text, contextID string) *protocol.SendMessageRequest {
	message := protocol.NewMessage(protocol.MessageRoleUser, protocol.NewTextPart(text))
	message.ContextID = contextID
	return &protocol.SendMessageRequest{Message: message}
}

func artifactText(artifacts []*protocol.Artifact) string {
	var result string
	for _, artifact := range artifacts {
		for _, part := range artifact.Parts {
			result += part.Text()
		}
	}
	return result
}

func mustWorkingDirectory(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

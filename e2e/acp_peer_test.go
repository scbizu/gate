package e2e_test

import (
	"github.com/anra-studio/gate/internal/testutil/acppeer"
	"os"
	"testing"
)

// TestACPAgentProcess is a fixture entry point, not a standalone assertion test.
// Gate launches this test binary as its ACP agent during BDD scenarios.
func TestACPAgentProcess(t *testing.T) {
	if os.Getenv(acppeer.Environment) != "1" {
		return
	}
	acppeer.Run()
}

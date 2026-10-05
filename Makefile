.PHONY: build e2e cli-test test proto-lint proto-build proto-generate a2a-connect-generate proto-breaking

# Keep existing make commands as aliases for the mise tasks.
build e2e cli-test test proto-lint proto-build proto-generate a2a-connect-generate proto-breaking:
	mise run $@

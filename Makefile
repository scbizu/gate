A2A_GIT_INPUT := https://github.com/a2aproject/A2A.git\#commit=173695755607e884aa9acf8ce4feed90e32727a1,subdir=specification,filter=tree:0

.PHONY: test proto-lint proto-build proto-generate proto-breaking

test: proto-lint proto-build
	go test ./...

proto-lint:
	buf lint

proto-build:
	buf build
	buf build '$(A2A_GIT_INPUT)'

proto-generate:
	buf generate --template buf.gen.yaml

# Compare against the main branch in CI. Override AGAINST for release branches.
proto-breaking:
	buf breaking --against "$${AGAINST:-.git#branch=main}"

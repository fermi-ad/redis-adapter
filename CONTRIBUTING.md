# Contributing

Contributions are welcome through GitHub issues and pull requests.

## Before opening a pull request

1. Discuss behavior or protocol changes in an issue first. Protocol changes
   should identify the affected specification section and compatibility impact.
2. Create a focused branch from `main`.
3. Clone or update submodules recursively.
4. Build and run the affected tests against Redis 7.4 or newer.
5. Update user-facing documentation and `CHANGELOG.md` when behavior changes.

See [Building from source](docs/building.md) for commands and dependencies.

## Pull requests

Keep changes focused and explain the user-visible result, validation performed,
and compatibility implications. Public pull-request CI builds on a GitHub-hosted
runner without institutional secrets.

By submitting a contribution, you agree that it may be distributed under this
project's [BSD 3-Clause License](LICENSE).

The organization-level code of conduct applies to this repository.

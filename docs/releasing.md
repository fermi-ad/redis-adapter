# Releasing RedisAdapter

RedisAdapter library releases use semantic versions. The wire-protocol version
changes only when the protocol specification changes.

## Prepare

1. Choose the semantic version and update [`VERSION.txt`](../VERSION.txt).
2. Add a dated entry to [`CHANGELOG.md`](../CHANGELOG.md).
3. Confirm dependency gitlinks and [`THIRD_PARTY_NOTICES.md`](../THIRD_PARTY_NOTICES.md)
   agree.
4. Run an anonymous recursive clone and the full test suite against Redis 7.4 or
   newer.
5. Merge the release-preparation pull request after public CI succeeds.

## Publish

Tag the exact commit contained in `main` and push the tag:

```sh
git switch main
git pull --ff-only
VERSION="$(tr -d '\n' < VERSION.txt)"
git tag -a "v${VERSION}" -m "redis-adapter v${VERSION}"
git push origin "v${VERSION}"
```

The release workflow rejects a tag when it does not match `VERSION.txt` or its
commit is not contained in `origin/main`. It then:

- builds and runs the complete suite against pinned Redis 7.4;
- creates `redis-adapter-<version>-source.tar.gz` with every pinned submodule;
- creates `SHA256SUMS`; and
- creates or updates the GitHub release and uploads both artifacts.

GitHub's automatically generated source archives do not contain git-submodule
contents. Consumers who want a complete build input should use the attached
`-source.tar.gz` artifact.

## Verify

After the workflow succeeds:

```sh
VERSION="$(tr -d '\n' < VERSION.txt)"
gh release view "v${VERSION}" --repo fermi-ad/redis-adapter
gh release download "v${VERSION}" --repo fermi-ad/redis-adapter --pattern 'redis-adapter-*'
sha256sum --check SHA256SUMS
```

On macOS, use `shasum -a 256 --check SHA256SUMS` for the final command.

Also confirm GitHub identifies the repository license as `BSD-3-Clause` and the
release source bundle contains `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES.md`, and
each dependency license listed there.

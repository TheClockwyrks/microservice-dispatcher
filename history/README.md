# history

The record of how this repository's history was extracted from the monorepo,
[The Test Cabinet](https://github.com/TheClockwyrks/TheTestCabinet), where the
dispatcher crate lived until the extraction. Nothing builds or tests against
these files; they are kept so the provenance outlives the scratch clone the
extraction ran in.

## The extraction

The history below the scaffold commit is the monorepo's `issue/repos-13` at
`bedd589f6a25bd0335c03530e2aba3895c75536e`, a branch of `nightly` that had not
yet changed anything, filtered to the paths in [`paths.txt`](paths.txt) by git
filter-repo 2.47.0:

```sh
git clone -q --no-local --single-branch --branch issue/repos-13 --no-tags \
  <the-test-cabinet> x
cd x
uvx --from git-filter-repo==2.47.0 git-filter-repo --force \
  --paths-from-file ../paths.txt --prune-empty always --preserve-commit-hashes
```

Of the 7,012 commits reachable from that commit, 74 touched a path in the list
and were kept (24 of them merges); the rest were pruned. No tag was carried.
The tip trees of `crates/dispatcher/` and `.env.dispatcher.example` hash the
same as the monorepo's at that commit.

The paths are kept where they were in the monorepo, so the filter needed no
rename.

## The files

- [`paths.txt`](paths.txt) is the list handed to `--paths-from-file`
  (sha256 `ceee8a58b9d5bc73d2b3729ad0dbe9b3f165487911be39c252961811f19ae327`):
  the crate's directory, `crates/dispatcher/`, and the environment example,
  `.env.dispatcher.example`. `git log --follow -M -C --name-status` over each
  of the 16 files found no path outside the two, so no historical path was
  added: the crate was written in place and never renamed or copied in.
- [`commit-map`](commit-map) is filter-repo's map from each monorepo commit
  (left) to the commit it became here (right), all zeros for a pruned one.
  `--preserve-commit-hashes` left the commit ids quoted in the messages as they
  were, so an id in a message below the scaffold commit names a monorepo
  commit, and this map translates it.

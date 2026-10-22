# Git and review additions in this fork

These additions build on [Waku by egoist](https://github.com/egoist/waku).
Waku already provides worktree creation, branch switching, commit and push,
checkpoint recovery, and several diff sources. Those are upstream features.
This increment extends the existing daemon and clients; it does not replace
Waku's architecture or claim full T3 Code feature parity.

## Availability

| Addition | Desktop | Web |
| --- | --- | --- |
| Stage or unstage individual files | Commit dialog | Commit dialog |
| Fast-forward-only pull | Commit dialog | Commit dialog |
| Upstream ahead/behind counts | — | Commit dialog |
| Inspect existing linked worktrees | — | Commit dialog |
| Ignore whitespace in review | Diff source menu | Diff source menu |

Use a desktop, daemon and web client built from this fork together. Protocol
v8 rejects mismatched clients instead of exposing controls backed by
unsupported daemon operations. The Expo mobile app was deleted (PLAN decision
D7); native mobile apps come later. No binary release of this fork is
published yet.

## Selecting files

File paths are relative to the repository root, including when the project is
a subdirectory. The two status characters describe the index and working tree;
for example, `M.` is staged, `.M` is unstaged, and `MM` has both staged and
unstaged changes. `??` is untracked. A checked file has staged changes; it does
not mean all its current contents are staged.

Selecting an individual file switches desktop and web commits to staged-only
mode. You can explicitly enable Include unstaged changes again. Unstaging
leaves working files intact.
Selection is whole-file; hunk staging and conflict resolution are not included.

The daemon validates the entire selection before changing the index, treats
pathspec-looking filenames literally, and handles both paths of an indexed
rename. Unsupported non-UTF-8 paths, oversized status output, and stale paths
produce an error. Refresh before retrying; failed mutations are not retried
automatically. Desktop displays five files per page; web virtualizes file
lists. Git operations run off the rendering thread.

## Pull and worktrees

Pull requires a committed, attached branch with an upstream and a clean working
tree, including untracked files. It permits only a fast-forward and disables
rebase and autostash. Divergent branches fail without initiating a merge.
Ahead/behind counts reflect locally known remote refs, not a background fetch.

The web worktree list shows branches, paths, and locks. This increment does not
delete or prune worktrees. Creation resolves the base to a commit and checks
that a nested project exists in that commit before creating a worktree.

Daemon workspace mutations serialize by shared Git directory, including linked
worktrees. This coordinates Waku requests; it is not a lock against external
editors, agent processes, or Git commands. Git hooks and configured helpers
retain normal Git behavior. These checks are not a sandbox for untrusted repos.

## Review

Ignore whitespace applies Git's `--ignore-all-space` consistently to the patch
and change counts. It changes the review view only, not files or commit content.
Use the ordinary view when whitespace is meaningful to the language or format.

PR creation, hosted review threads, merge controls, worktree reuse/removal,
setup scripts, and hunk staging remain outside this increment. The native
changes have compilation coverage; interactive desktop and device validation
is still required before a release.

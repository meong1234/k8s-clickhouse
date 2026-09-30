# Claude instructions — k8s-clickhouse

## Never read the `.ignore/` folder

The `.ignore/` directory at the repo root is a **local scratch / dump folder**. It is
git-ignored and may hold unrelated, large, or sensitive files.

**Never read, open, list, search, or otherwise access anything under `.ignore/`** — not
with Read, Grep, Glob, or Bash (`cat`, `less`, `head`, `tail`, `find`, `rg`, `ls`, etc.),
and do not include it when searching or globbing the repo. Treat it as out of scope for
every task. The only exception is if, in the current request, the user explicitly names a
specific file inside `.ignore/` and asks you to work with it.

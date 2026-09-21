---
title: Credential handling
description: Local credential bootstrap, scanner triage, and history-cleanup boundaries.
audience: developer
---

# Credential handling

The repository stores credential names and bootstrap logic, but no credential values. Run
`uv run invoke init-secrets` to create strong missing values in the ignored local `.env` file.
Do not copy local values into Compose files, inventories, examples, tests, or documentation.

## Credential scanner triage

Review scanner matches in context before suppressing a rule. A reviewed security report identified
nine `PASSWORD` matches for the word `semaphore` in `tasks.py`. Those occurrences are ordinary
identifiers, task names, paths, decorators, and comments. They do not contain credential values and
remain intentionally unchanged.

Do not suppress or globally replace `semaphore`. A real fixed Semaphore password in the same file was
removed, so a broad exception would hide the distinction between an identifier and a credential.

## Git history boundary

Removing a value from the current tree does not remove it from reachable Git objects. Historical
cleanup requires a coordinated maintainer rewrite, rewritten branch and tag force-pushes, fork
cleanup, fresh clones, and removal of cached GitHub views when applicable. Never describe a normal
pull request as erasing repository history.

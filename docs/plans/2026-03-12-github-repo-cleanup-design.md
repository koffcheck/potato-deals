# GitHub Repository Cleanup Design

**Goal:** Make the published GitHub repository understandable for non-technical users and remove outdated artifacts that create download confusion.

**Context**

The repository root currently contains an old release archive and a README that points to an outdated direct download from a different repository. This creates two separate sources of confusion:

- users can download an outdated ZIP from the repository root
- users can follow a README link that points outside the current repository

**Chosen Approach**

Use an English-first README focused on three questions:

- what to download
- what not to download
- how to install it

At the same time, remove the tracked old ZIP from the repository root so the only intended download path is the GitHub Releases page.

**Scope**

- rewrite `README.md`
- stop tracking the old root ZIP archive
- remove the `.gitignore` exception that preserves the old ZIP
- add a reusable release notes template for future releases

**Out of Scope**

- changing plugin code
- rebuilding the plugin
- editing already published GitHub release pages automatically

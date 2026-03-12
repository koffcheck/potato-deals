# GitHub Repository Cleanup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Clean up the published repository so new visitors download the correct release ZIP from GitHub Releases instead of outdated or incorrect files.

**Architecture:** The repository landing page will become a short download-first README. The outdated ZIP in the repository root will be removed from version control, and a reusable release template will document the correct future publishing pattern.

**Tech Stack:** Markdown, Git, GitHub repository metadata

---

### Task 1: Rewrite the README

**Files:**
- Modify: `README.md`

**Step 1: Replace the outdated download link**

Point the main download action to the current repository releases page instead of the old `openanton` raw link.

**Step 2: Make the first screen download-first**

Put a direct callout at the top that explains:
- download only the release ZIP
- do not use `Source code` archives

**Step 3: Keep instructions short**

Use a compact install section with 4 steps and no unnecessary extra text.

### Task 2: Remove outdated ZIP handling

**Files:**
- Modify: `.gitignore`
- Delete: `potato-deals-v3.1.14.zip`

**Step 1: Remove the ZIP exception**

Delete the tracked exception for the old root ZIP file from `.gitignore`.

**Step 2: Remove the old artifact from the repository tree**

Delete `potato-deals-v3.1.14.zip` so users stop treating the repository root as the install source.

### Task 3: Add a release template

**Files:**
- Create: `docs/release-notes-template.md`

**Step 1: Add a simple reusable template**

Document:
- the correct release ZIP filename
- a warning not to use source archives
- install steps
- a short changelog section

### Task 4: Validate and ship

**Files:**
- Review: `README.md`
- Review: `.gitignore`
- Review: `docs/release-notes-template.md`

**Step 1: Confirm beginner clarity**

Verify that the first visible README section explains what to download and what not to download.

**Step 2: Confirm repository cleanup**

Verify that the old ZIP is removed from the tracked tree.

**Step 3: Commit and push**

Create a focused commit and push to `origin/main`.

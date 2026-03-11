---
name: review
description: >
  Review an open pull request for guide compliance, correctness, and code
  quality. Use this skill whenever the user says "/review <number>",
  "review PR #N", "review this PR", or similar. Fetches the PR diff and
  metadata, reads the project guides and source files under review, analyzes
  findings by severity, presents them for approval, and posts an inline
  review via the GitHub API.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT, REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be interpreted as described in RFC 2119.

# Review Skill

Fetch a pull request, analyze its diff against project guides and source context, categorize findings by severity, present them for user approval, and post an inline GitHub review.

## Pipeline Context

This skill is part of the development workflow pipeline: `/issue` → `/pr` → `/implement` → `/commit` → `/pr` (update) → `/review`. This skill is the **sixth** stage, invoked after the PR has been updated and is ready for review.

## Arguments

A PR number MUST be provided as the sole argument (e.g., `/review 146`).

## Workflow

### TL;DR

1. Fetch the PR metadata and diff
2. Read project guides and styles
3. Read source files under review
4. Analyze the diff
5. Categorize findings
6. Present findings for approval
7. Post the review
8. Prompt the user with next steps

### 1. Fetch the PR metadata and diff

```bash
gh pr view <number> --json title,body,headRefName,baseRefName,changedFiles
gh pr diff <number>
```

If the PR does not exist, inform the user and stop. Parse the PR title, body, branch names, and the list of changed files.

### 2. Read project guides and styles

Read the following files to establish the review baseline:

- `CLAUDE.md` — project-level instructions, architecture context, docstring conventions.
- `llm/guides/testguide-python.md` — Python testing conventions (if the PR touches test files).
- Every file in `llm/styles/` — active authoring conventions.

Only the guides relevant to the changed files need to be read. For example, the test guide is only needed if the PR modifies or adds test files.

### 3. Read source files under review

For each changed file in the PR:

- Read the full file at its current HEAD revision so that surrounding context is available.
- If the changed file is a test file, also read the corresponding source module it tests (and vice versa).

### 4. Analyze the diff

Review every hunk in the diff against the guides read in step 2 and the source context read in step 3. Check for:

- **Guide compliance** — violations of MUST/SHALL/SHOULD rules from `CLAUDE.md`, the test guide, and style guides. Cite the specific guide rule being violated.
- **Naming and conventions** — inconsistent naming, style drift from the surrounding codebase, or departures from documented conventions.
- **Coverage regressions** — new public APIs without corresponding tests, removed tests without justification, or test gaps relative to the PR's own test cases table.
- **Correctness bugs** — logic errors, race conditions, missing error handling at system boundaries, incorrect use of APIs or libraries.
- **Code quality** — unnecessary complexity, dead code, duplicated logic, or poor separation of concerns.

Each finding MUST reference the specific file and line range in the diff where the issue occurs.

### 5. Categorize findings

Every finding MUST be assigned exactly one severity:

- **Blocking** — Violations of MUST or SHALL requirements from any project guide. These MUST be resolved before the PR can be approved.
- **Non-blocking** — Violations of SHOULD or RECOMMENDED requirements, style suggestions, or minor quality improvements. These are advisory and do not block approval.

Present findings grouped by severity, with blocking findings first.

### 6. Present findings for approval

The complete list of findings MUST be presented to the user before posting. For each finding, show:

- Severity (blocking / non-blocking)
- File and line range
- Description of the issue
- The comment text that will be posted

The user MUST be given the opportunity to:

- **Remove** any finding they disagree with.
- **Edit** the comment text of any finding.
- **Add** new findings the agent missed.
- **Change** the severity of any finding.

The review MUST NOT be posted until the user explicitly approves the final set of findings.

### 7. Post the review

Construct a review payload and post it via the GitHub API. The payload MUST be written to a temporary file to avoid shell escaping issues:

```bash
cat > /tmp/review_payload.json << 'EOF'
{
  "event": "<APPROVE|REQUEST_CHANGES|COMMENT>",
  "body": "",
  "comments": [
    {
      "path": "relative/path/to/file.py",
      "line": 42,
      "body": "Comment text here."
    }
  ]
}
EOF
gh api repos/{owner}/{repo}/pulls/<number>/reviews \
  --method POST --input /tmp/review_payload.json
```

**Review event selection:**

- If there are any **blocking** findings remaining after user approval, the event MUST be `REQUEST_CHANGES`.
- If there are only **non-blocking** findings, the event SHOULD be `COMMENT`.
- If there are no findings, the event SHOULD be `APPROVE`.

**Review body:** The top-level review `body` SHOULD be an empty string when all findings are line-specific inline comments. A non-empty body is acceptable for findings that cannot be associated with a specific changed line — for example, something from the issue that the PR completely missed, or an architectural concern that spans the entire diff. The user decides whether to use a top-level comment — defer to them.

**Comment text:** Each inline comment MUST contain only the substantive feedback. Comments MUST NOT include pseudo-headers, severity labels, category tags, or other metadata — just the review feedback itself.

**Multi-line comments:** When a finding spans multiple lines, use the `start_line` and `line` fields to highlight the full range:

```json
{
  "path": "relative/path/to/file.py",
  "start_line": 10,
  "line": 15,
  "body": "Comment text here."
}
```

### 8. Prompt the user with next steps

After the review is posted, prompt the user with the appropriate next step:

- If the review event was `REQUEST_CHANGES` or `COMMENT`: "Review posted. Run `/audit review` to verify compliance, or address the findings with `/implement <number>` and re-run `/review <number>` after pushing fixes."
- If the review event was `APPROVE`: "PR approved. Run `gh pr ready <number>` to mark it ready for merge, or `/audit review` to verify the review was thorough."

DO NOT proceed on your own.

## Edge Cases

**PR is already merged or closed:** Inform the user that the PR is not open and stop.

**No findings:** If the diff passes all checks cleanly, post an `APPROVE` review with an empty body and no inline comments. Inform the user that no issues were found.

**Binary files in diff:** Binary files MUST be skipped during analysis. Note their presence to the user but do not attempt to review them.

**Very large diffs:** For PRs with more than 20 changed files or more than 1000 lines changed, the agent SHOULD summarize the scope to the user and ask whether to review the full diff or focus on specific files.

**Files outside the repository's guide coverage:** If changed files are in a language or domain not covered by any project guide, review them for general correctness and code quality only. Do not fabricate guide requirements that do not exist.

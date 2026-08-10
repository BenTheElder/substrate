---
name: post-draft-review
description: Posts pull request review findings as GitHub draft (pending) inline comments for a human to finish and submit.
---

# Post Draft Review Comments

A "draft review" on GitHub is a review left in the **`PENDING`** state. It is visible
only to its author, accumulating comments, until the author explicitly **submits**
(publishes) or **discards** it.

Use this skill when asked to review a pull request. Findings go in as pending inline
comments rather than individually published comments, so a human reviews them before
anything reaches the PR author.

`gh pr review` can **not** create one — it always submits immediately
(`--approve` / `--comment` / `--request-changes`). To stay in draft, hit the REST API:
`POST .../reviews` **with no `event` field** creates the review `PENDING` rather than
submitting it.

## Inline comments only — the human authors the summary

Contribute **inline comments only**, created with an **empty body** (`body: ""`). The
top-level summary is written by the human in the UI's "Finish your review" box when they
submit. Rationale:

- The high-level overview (praise, verdict, framing) is the human's voice, not the
  agent's.
- The UI's summary box does **not** prefill an API-set body, so an agent-authored body
  is silently dropped on UI submit anyway, and can't be restored afterwards: `PUT` on a
  submitted review with an empty body is rejected with 422. An empty body sidesteps the
  whole failure mode.
- Don't smuggle summary-level content into an inline comment. Top-level content anchored
  to a code line is poor UX for the reader.

Anything with no inline anchor — what was checked and found clean, issues in files
outside the diff — goes in the chat hand-off, for the human to draw on when they write
the summary.

## Disclose that an agent found it

Each inline finding leads with `:robot:` followed by the bolded severity name and color
(e.g. `🤖 **should-fix** 🟡 – …`) to mark it as an AI finding. Keep it generic: don't
name the agent, vendor, or model. Disclose only that *an* agent was involved. The human
who finishes the review is accountable for what gets submitted.

## Keep it tight

Bias toward brevity. A human reads this, then decides. Concretely:

- **Inline comments**: lead with the severity tag and state the problem in a sentence or
  two, then the fix. Drop preamble, restated context, and pasted command/test output —
  point at the line and trust the reader.
- **Short sentences.** One clause each where possible; no run-ons or long em-dash chains.
  State the claim and its consequence; leave the derivation (timelines, worked examples,
  step-by-step traces) out. The author can re-derive it from the claim, and can ask if
  they can't.
- **Severity tags**: blocking 🔴 · should-fix 🟡 · nit 🟢 · question 🟢. Every inline
  finding opens with the robot marker, then the **bolded** severity name, then the color:
  `🤖 **blocking** 🔴 – …`, `🤖 **should-fix** 🟡 – …`, `🤖 **nit** 🟢 – …`,
  `🤖 **question** 🟢 – …`. Leading with 🤖 makes agent comments uniformly recognizable.
  The severity *name* is required because the colors alone aren't a standard PR authors
  know, and a bare 🟢 beside a critique reads like approval.
- When in doubt, cut. A finding the reader can act on in five seconds beats a paragraph.
- **Brevity comes from cutting content, not compressing prose.** Humans read these: keep
  complete sentences. No telegraphic fragments, abbreviations ("incl.", "w/"), or bare
  "label: clause" constructions. Drop whole points that don't change what the reader does
  next; write the surviving ones plainly.

## Create the draft (PENDING) review

Write each comment body to a file and assemble the payload with `jq --rawfile`, which
escapes markdown, backticks, and newlines correctly. The body is always empty:

```bash
jq -n --rawfile c1 c1.md --rawfile c2 c2.md '{
  body: "",
  comments: [
    {path:"cmd/ateapi/internal/store/ateredis/ateredis.go", line:849, side:"RIGHT", body:$c1},
    {path:"cmd/ateapi/main.go", line:57, side:"RIGHT", body:$c2}
  ]
}' > /tmp/review.json

gh api repos/<owner>/<repo>/pulls/<num>/reviews \
  --method POST --input /tmp/review.json --jq '{id, state}'
# => {"id": ..., "state": "PENDING"}
```

`state: PENDING` confirms it's a draft: not visible to anyone else, no notifications,
until submitted. Keep the returned `id` to inspect or discard. Omitting `event` is what
keeps it in draft; including `event` submits immediately.

Inline-comment notes:

- `line` is the line number **in the file at the PR head**. `side: "RIGHT"` is the new
  (post-change) version, `"LEFT"` the base side.
- Multi-line: use `start_line` and `start_side` together with `line`/`side`.
- The line **must fall within the PR's diff hunk**, or the API rejects the whole review.
  A file that isn't in the diff at all (for example a caller that wasn't touched) can't
  take an inline comment. Anchor the point on a *changed* line nearby, such as the struct
  field that the untouched caller fails to populate, and reference the real location in
  prose. Otherwise leave it to the chat hand-off.
- Pin exact head line numbers from a checkout of the PR head (`gh pr checkout <num>`, or
  fetch `pull/<num>/head`). Don't eyeball them from the diff.

### Editing a draft you already created

There's no clean way to add or change inline comments on an existing PENDING review.
**Delete and recreate** it with the full `comments` array. It's still a draft, so nothing
was published. The review id changes:

```bash
gh api repos/<owner>/<repo>/pulls/<num>/reviews/<old_id> --method DELETE
gh api repos/<owner>/<repo>/pulls/<num>/reviews --method POST --input /tmp/review.json
```

### Verify anchors landed correctly

`line` and `original_line` come back `null` while a review is PENDING (they resolve on
submit), and `position` is a legacy cumulative diff offset. Neither tells you the file
line at a glance. Instead, check the **last line of each comment's `diff_hunk`**, which
is the line the comment is attached to:

```bash
gh api repos/<owner>/<repo>/pulls/<num>/reviews/<id>/comments \
  --jq '.[] | "\(.path)\n   ↳ \(.diff_hunk | split("\n") | last)\n"'
```

## Hand-off, then the human finishes

The hand-off in chat should give the human what they need to write the summary and
decide: a short findings table (severity, one-line claim, `file:line` anchor) and the
checked-and-found-clean notes, meaning the verification work with no inline anchor. No
paste-ready summary text — the summary is the human's to write.

The human then reviews the pending comments in the GitHub UI (the PR shows a **Pending**
badge with a **Finish your review** button), edits or deletes any of them, writes their
own summary in the box, and submits. Since the draft has no body, nothing can be lost on
UI submit.

To discard the draft instead:

```bash
gh api repos/<owner>/<repo>/pulls/<num>/reviews/<review_id> --method DELETE
```

## Gotchas

- Each author may have only **one** pending review per PR at a time. A second
  `POST .../reviews` while one is pending errors out. Submit or delete the first.
- Relative markdown links in comment bodies (for example `[x](cmd/.../foo.go#L1)`) render
  oddly on GitHub. Prefer plain `` `path:line` `` in backticks for code references.

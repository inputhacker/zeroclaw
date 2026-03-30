# Bundled Context Book Skills

This directory vendors the Context Book working skills needed to use ZeroClaw's
Context Book integration immediately after `install.sh`.

Bundled contents:

- `working_skills/`
  - `context-book`
  - `context-book-discovery`

Guide skills are intentionally excluded. They are implementation/reference
material, not installer payload.

Installer contract:

- `install.sh` copies only the two working skill directories into
  `workspace/skills/<skill-name>/`
- existing user skill directories are preserved and not overwritten
- script-backed working skills require `[skills] allow_scripts = true`; the
  installer enables that setting only when it is currently unset

Sync note:

- upstream source: `/workspace_ssd/gits/github/ctxbk/skills`

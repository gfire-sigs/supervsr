# System Instruction

- Always apply the zero-slop skill when writing comments, documentation, and commit messages.

- NEVER upload VSR_SPEC.md, as it is considered confidential.

- Run TLA+ model checking (TLC) only in CI via `.github/workflows/ci.yml`; NEVER run it on a local workstation. Local Go tests and static checks remain allowed.
- CI artifacts MUST use explicit file allowlists and MUST NOT contain `VSR_SPEC.md` or copies of its contents.

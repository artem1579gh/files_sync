# files_sync: session plan

Run one session per row, each starting from a fresh context. Task details are in [`tasks.md`](tasks.md) and the design is in [`design.md`](design.md).

Prompt to start a session:

> Do tasks T0X–T0Y from `claude/tasks.md`. Follow `CLAUDE.md`.

After each session, review the commit and the task's **Notes** field before starting the next one.

| Session | Tasks | Why together |
|---|---|---|
| 1 | T01 + T02 | small scaffolding |
| 2 | T03 | the foundation; worth reviewing alone |
| 3 | T04 + T05 | the temp-file and swap logic, tightly coupled |
| 4 | T06 + T07 | delete, then the attack test suite across all operations |
| 5 | T08 + T09 | independent pure logic |
| 6+ | one task each (T10 → T22) | larger tasks |

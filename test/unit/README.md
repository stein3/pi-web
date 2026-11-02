# Unit layer (fast)

Sub-second tests that import `web/lib/*.mjs` directly — no spawned pi-web,
no fake pi, no ports. Run with `cd web && npm run test:unit`.

The integration layer lives in `test/*.mjs` (the full serial suite, ~5 min)
and runs via `npm run test:integration` (= `npm run check`). CI runs both:
`npm run test:all`.

Both layers are required by the standing SWE rule (global AGENTS.md): a bug
needs a failing unit test first, and the real wiring needs integration coverage.

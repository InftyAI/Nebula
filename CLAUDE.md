# Nebula

## Comments

This codebase comments the *why*, not the *what*. Match that — and match its density too,
don't exceed it.

- Never restate the code. `// increment the counter` above `n++` is noise.
- Don't repeat a rationale a nearby doc comment already gives. Name the symbol that holds
  it (`see Offering.GPUCount`) and move on.
- Doc comments: the contract, plus the one constraint a caller would otherwise get wrong.
  2-5 lines. Go longer only for a real invariant — a subtle failure mode, a decision kept
  on purpose, a gotcha in an external API.
- Inline comments: only where a reader would otherwise draw the wrong conclusion. Not one
  per branch, not one per field assignment.
- One comment on the tricky invariant beats five on the obvious steps.

If a comment is longer than the code it explains, it is usually the wrong comment.

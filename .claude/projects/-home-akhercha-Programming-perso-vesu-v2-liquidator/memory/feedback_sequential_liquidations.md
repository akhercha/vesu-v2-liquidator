---
name: Sequential liquidations
description: Liquidations must be executed sequentially, never in parallel with tokio::spawn
type: feedback
---

Liquidations must be sequential, not parallel.

**Why:** Parallel liquidation spawns hammer the Ekubo API and cause duplicate attempts on the same positions. The user strongly prefers sequential execution.

**How to apply:** Never use `tokio::spawn` for liquidation tasks. Collect liquidable positions, then iterate and await each one sequentially in the monitoring loop.

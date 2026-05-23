---
trigger: model_decision
description: Follow these steps before declaring the work as "done".
---

# Definition of Done (DoD)

Before marking any task as complete or concluding your work, you MUST run and pass the following verification steps locally. These steps match the GitHub Actions CI workflow configured in [.github/workflows/rust.yml](../../.github/workflows/rust.yml).

### 1. Build Verification
Ensure the codebase compiles successfully:
```bash
cargo build
```

### 2. Code Formatting Check
Format all code files and ensure compliance:
```bash
cargo fmt --all -- --check
```

### 3. Linter & Code Quality
Run Clippy and treat all warnings as errors:
```bash
cargo clippy -- -D warnings
```

### 4. Test Suite Execution
Run all unit and doc tests:
```bash
cargo test
```
*Note: Running `cargo test` requires an active Redis server running locally on the default port `6379`.*

### 5. Local Development Prerequisites
If a Redis server is not already running locally on port 6379, start one (e.g. using Docker):
```bash
docker run -d -p 6379:6379 redis:7
```

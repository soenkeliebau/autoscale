# Rust Development Guidelines

## Build & Validation Commands

Always run these before considering any task complete:
```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo build
```

For library crates, also check documentation:
```bash
cargo doc --no-deps --all-features 2>&1 | grep -E "^warning" && echo "Doc warnings found" || echo "Docs clean"
```

---

## Error Handling with `snafu`

All error handling **must** use [`snafu`](https://docs.rs/snafu). No `unwrap()`, `expect()`, or `Box<dyn Error>` in library code.

### Error Enum Structure

Every module with fallible operations defines its own `Error` enum in that module:
```rust
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to read config file at {path:?}"))]
    ReadConfig {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Invalid value {value} for field '{field}': {reason}"))]
    InvalidValue {
        value: String,
        field: &'static str,
        reason: String,
    },

    #[snafu(display("Timeout after {duration:?} waiting for {operation}"))]
    Timeout {
        duration: std::time::Duration,
        operation: String,
    },
}

pub type Result = std::result::Result;
```

### Rules

1. **Always include `source`** for errors that wrap another error. Never discard the original error.
2. **Always include context fields** that help diagnose the failure (path, key, index, etc.).
3. **Use `#[snafu(display(...))]`** on every variant — messages must be human-readable and include the relevant context fields.
4. **Use the `context()` / `whatever!()` extension methods**, never `map_err(|e| Error::Foo { source: e })` manually:
```rust
use snafu::ResultExt;

// Good
std::fs::read_to_string(&path).context(ReadConfigSnafu { path: &path })?;

// Bad
std::fs::read_to_string(&path).map_err(|e| Error::ReadConfig { path: path.clone(), source: e })?;
```

5. **Module-local error types**: Each module (or logical subsystem) has its own `Error` + `Result`. Callers wrap lower-level errors with a new variant that includes `source`:
```rust
// In parent module
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Configuration error"))]
    Config { source: crate::config::Error },
}
```

6. **No stringly-typed errors**: Avoid `snafu::Whatever` in library code. Use it only in `main()` or test helpers.
7. **`source` field must always be named `source`** so snafu can auto-propagate it in `std::error::Error::source()`.

---

## Interface Design

### Public API

- **Prefer returning `impl Trait` over concrete types** where the concrete type is an implementation detail.
- **Use the typestate pattern** for builders and state machines to enforce valid usage at compile time.
- **Avoid `&str` parameters that require owned data** — prefer `impl Into<String>` or `impl AsRef<str>` at the boundary.
- **Use `impl AsRef<Path>`** instead of `&Path` or `&str` for filesystem paths.
- **Prefer `&[T]` over `&Vec<T>`** and `&str` over `&String` in function signatures.
- **`Default` should be implemented** for any configuration/options struct.
- **Builder pattern for structs with 3+ optional fields**; derive or implement `Default` on the builder.

### Naming

- Types, traits: `UpperCamelCase`
- Functions, methods, variables, modules: `snake_case`
- Constants, statics: `SCREAMING_SNAKE_CASE`
- Error variants: descriptive nouns, not `IoError` but `ReadConfig`, `WriteOutput`, etc.
- Boolean parameters: avoid — use an enum or separate methods instead.

### Trait Implementations

Implement standard traits wherever it makes sense:
- `Debug` on everything (derive unless custom formatting is needed)
- `Clone` when copying is cheap or needed
- `Display` on user-facing types
- `From` / `TryFrom` for conversions, not bespoke `to_x()` / `from_x()` methods
- `PartialEq` / `Eq` on value types used in tests or comparisons

---

## Code Style

### Formatting

- `rustfmt` is non-negotiable. Never manually format code that `rustfmt` would change.
- Use a `rustfmt.toml` if project-specific settings are needed; document why.

### Clippy

Zero clippy warnings. Suppressions via `#[allow(...)]` must have a comment explaining why:
```rust
// `clippy::too_many_arguments` suppressed: this is a low-level syscall wrapper
// where grouping into a struct would obscure the 1:1 mapping to the API.
#[allow(clippy::too_many_arguments)]
fn raw_call(...) {}
```

Do not suppress:
- `clippy::unwrap_used` — fix it instead
- `clippy::expect_used` in library code — fix it instead
- `clippy::panic` in library code — fix it instead

### Panics

- **Never panic in library code** unless the invariant violation is a programming error (not a user/environment error).
- Document any `panic!` or `unreachable!` with `// PANIC: <reason this is unreachable>`.
- Prefer `debug_assert!` over `assert!` for internal invariants that should be checked only in debug builds.

### Unwrap / Expect

- `unwrap()` / `expect()` are forbidden in library code.
- In test code, `unwrap()` is acceptable but `expect("context")` is preferred.
- In `main()`, use `?` propagation to a top-level `snafu::Whatever` or your binary's own `Error`.

---

## Project Structure
```
src/
  lib.rs          # Re-exports, top-level doc comment, no logic
  error.rs        # Top-level Error + Result (if needed)
  config/
    mod.rs
    error.rs      # config::Error
  client/
    mod.rs
    error.rs      # client::Error
  ...
```

- Modules map to files or directories — no `mod foo { ... }` inline blocks for non-trivial code.
- `pub use` at the crate root to flatten the public API surface.
- `#[doc(hidden)]` on items that must be `pub` for macro reasons but aren't part of the API.

---

## Dependencies

- Prefer the standard library over external crates for simple tasks.
- Pin major versions in `Cargo.toml`; use `cargo update` deliberately.
- Every added dependency must justify its inclusion: size, maintenance status, license.
- Avoid multiple crates solving the same problem (e.g., two async runtimes, two HTTP clients).

---

## Tests

- Unit tests live in `#[cfg(test)] mod tests { ... }` at the bottom of the file they test.
- Integration tests live in `tests/`.
- Test names describe the scenario: `returns_error_when_config_missing`, not `test1`.
- Use `assert_matches!` (or `matches!`) for pattern-matching on `Result`/`Option` variants.
- Test error *types*, not just `is_err()`:
```rust
assert!(matches!(result, Err(Error::ReadConfig { .. })));
```

---

## Documentation

- Every `pub` item must have a doc comment (`///`).
- Doc comments use full sentences ending in `.`.
- Include `# Errors` section on fallible functions listing possible error variants.
- Include `# Panics` section if the function can panic.
- `# Examples` section with a runnable `doctest` on non-trivial public functions.
```rust
/// Loads configuration from the given path.
///
/// # Errors
///
/// Returns [`Error::ReadConfig`] if the file cannot be read.
/// Returns [`Error::ParseConfig`] if the file content is not valid TOML.
pub fn load(path: impl AsRef) -> Result { ... }
```
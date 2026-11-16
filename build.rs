fn main() {
    // Header generation is opt-in via the `header` feature (same pattern
    // as hakodb 0.20.4): release/tag CI and sync scripts build with
    // `--features header`, everything else compiles zero header
    // machinery — the cbindgen build-dependency is optional and stays
    // off. HAKOCLUSTER_SKIP_CBINDGEN=1 still forces a skip in
    // header-enabled dev loops; set per-invocation, not globally.
    // Link-search + lib hygiene below always run (dependents link
    // against them even in dev loops).
    #[cfg(feature = "header")]
    if std::env::var("HAKOCLUSTER_SKIP_CBINDGEN").is_err() {
        gen_header();
    }
    println!("cargo:rerun-if-changed=src/");
    println!("cargo:rerun-if-changed=cbindgen.toml");

    // One line of truth in every build's output: protects the header flow
    // from silent regressions (the 1.100 layout change produced none).
    println!(
        "cargo:warning=hakocluster build-script: header feature={} skip_env={}",
        std::env::var_os("CARGO_FEATURE_HEADER").is_some(),
        std::env::var_os("HAKOCLUSTER_SKIP_CBINDGEN").is_some(),
    );

    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "release".to_string());
    let target_dir = std::path::Path::new("target").join(&profile);

// ponytail: a stale unprefixed import lib SHADOWS the fresh .dll in
// MinGW ld search order (it ran BEFORE rustc linked, so it always lagged
// a build behind), producing undefined-reference ghosts for new symbols.
// Delete it — MinGW ld falls through to hakocluster.dll directly (always
// fresh). MSVC consumers link rustc's `hakocluster.dll.lib`, so the
// delete below runs on GNU/MinGW targets only.
#[cfg(windows)]
{
let target = std::env::var("TARGET").unwrap_or_default();
if !target.contains("msvc") {
    let dst = target_dir.join("hakocluster.lib");
    let _ = std::fs::remove_file(&dst);
}
}
    // Allow integration tests to find hakocluster.dll when invoked anywhere.
    println!("cargo:rustc-link-search=native={}", target_dir.display());
}

#[cfg(feature = "header")]
fn gen_header() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir should exist");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR should exist");
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "release".to_string());
    let output = std::path::Path::new(&out_dir).join("hakocluster.h");

    // ponytail: absolute path — a relative "cbindgen.toml" silently misses
    // (build-script cwd is not guaranteed to be the package root), and
    // from_file().unwrap_or_default() then falls back to cbindgen DEFAULTS
    // with zero diagnostics. Absolute path or loud failure, never silent
    // defaults for a shipped ABI header.
    let cbindgen_toml = std::path::Path::new(&crate_dir).join("cbindgen.toml");
    let cfg = cbindgen::Config::from_file(&cbindgen_toml)
        .map_err(|e| format!("read {}: {e}", cbindgen_toml.display()))
        .expect("cbindgen.toml must load — refusing silent-default ABI header");
    cbindgen::Builder::new()
        .with_crate(crate_dir.clone())
        .with_config(cfg)
        .generate()
        .expect("Unable to generate C header")
        .write_to_file(&output);
    // ponytail: stable per-target copy next to the binaries. OUT_DIR hides
    // under build/<pkg>/<hash>/out (cargo 1.100 layout; formerly
    // build/<pkg>-<hash>/out) — unusable as a consumer path, and a fixed
    // ancestor count no longer points at target/<profile>/. So the copy
    // target is the profile dir found by NAME, and both the generate and
    // copy steps fail loudly (silence here shipped broken releases).
    // No checked-in header (drift-proof: declarations always match the
    // built cdylib).
    let profile_dir = profile_dir_of(&out_dir, &profile)
        .expect("profile dir not found above OUT_DIR - refusing to ship a header consumers can't find");
    std::fs::copy(&output, profile_dir.join("hakocluster.h"))
        .expect("Unable to copy C header");
}

/// The profile dir (`target/<profile>/`) above OUT_DIR, located by NAME:
/// the first ancestor whose final component matches the PROFILE env value
/// ("release"/"debug"). Depth-independent - survives cargo layout changes
/// (same fix as hakodb 0.20.3).
#[cfg(feature = "header")]
fn profile_dir_of(out_dir: &str, profile: &str) -> Option<std::path::PathBuf> {
    std::path::Path::new(out_dir)
        .ancestors()
        .find(|p| p.file_name().is_some_and(|n| n == profile))
        .map(|p| p.to_path_buf())
}

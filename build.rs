fn main() {
    // Dev-loop shortcut (same pattern as hakodb): HAKOCLUSTER_SKIP_CBINDGEN=1
    // skips header generation (seconds per rebuild on big trees).
    // CI/release never set it, so shipped headers always regenerate.
    // Link-search + lib hygiene below always run (dependents link
    // against them even in dev loops).
    if std::env::var("HAKOCLUSTER_SKIP_CBINDGEN").is_err() {
        gen_header();
    }
    println!("cargo:rerun-if-changed=src/");
    println!("cargo:rerun-if-changed=cbindgen.toml");

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

fn gen_header() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir should exist");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR should exist");
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
    // under target/<triple>/<profile>/build/<pkg>-<hash>/out — unusable as
    // a consumer path. The profile dir (3 ancestors up) is stable across
    // hosts, triples, and profiles, so release CI, sync scripts, and the
    // C smoke test all take `target/<...>/hakocluster.h` from the build
    // that produced it. No checked-in header (drift-proof: declarations
    // always match the built cdylib).
    if let Some(profile_dir) = std::path::Path::new(&out_dir).ancestors().nth(3) {
        let _ = std::fs::copy(&output, profile_dir.join("hakocluster.h"));
    }
}

# Fork policy

This repository is the `leiysky/foyer` fork used to develop and validate the Extent disk engine.
The fork keeps Foyer's public hybrid-cache layer and makes the disk-engine boundary reusable by
BlockEngine and ExtentEngine.

## Baseline

- Upstream: `https://github.com/foyer-rs/foyer.git`
- Release: `v0.22.3`
- Commit: `ff6b01512e580665a217c2bd892e0a884ae749e6`
- Baseline date: 2026-01-23

`v0.22.3` is the latest formal upstream release at the point the fork is established. Upstream
`main` is already on the moving `0.23.0-dev` line, but none of its post-release changes is a
prerequisite for Extent. Starting from the release tag gives the fork a reproducible compatibility
boundary and avoids mixing an engine change with unrelated unreleased dependency and policy work.

## Branch and remote policy

- `origin` is `https://github.com/leiysky/foyer.git`.
- `upstream` is `https://github.com/foyer-rs/foyer.git`.
- Fork development starts on `dev/extent-engine-api` from the exact baseline commit above.
- Upstream changes are reviewed and rebased or cherry-picked deliberately. Do not merge a moving
  upstream development branch into the fork merely to make the histories appear current.

The fork's `main` branch is fast-forwarded to the baseline release before the development branch is
published. Future baseline changes require an explicit compatibility review of the disk-engine API,
the on-disk Extent format, and the fork MSRV.

## Distribution boundary

`foyer`, `foyer-extent`, and `foyer-fixed-lsm` are versioned as one source revision. The Extent
packages are workspace-private and are not published independently. External Extent users must pin
the complete fork revision rather than combining crates.io Foyer with a fork engine crate.

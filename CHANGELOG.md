# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0](https://github.com/0xCCF4/zrb/compare/v0.2.0...v0.3.0) - 2026-06-01

### Fixed

- ci tests
- revisited snapshot command and fixed tui formatting bugs
- split send and snapshot into two different commands

### Other

- *(deps)* bump log from 0.4.29 to 0.4.30
- progress bars
- use zfs json output instead of human readable cli out

## [0.2.0](https://github.com/0xCCF4/zrb/compare/v0.1.1...v0.2.0) - 2026-05-30

### Fixed

- hold lingers bug
- tests break on version update
- [**breaking**] hold most recent snapshot to prevent history divergence
- prune
- tokio runtime
- tokio runtime

### Other

- tui
- progress bars
- [**breaking**] pretty prune and fix deadlock for resuming finished transfer
- use tokio for sending to multiple hosts

## [0.1.1](https://github.com/0xCCF4/zrb/compare/v0.1.0...v0.1.1) - 2026-05-25

### Fixed

- small cli inconveniences
- noxa module
- ci tests ([#10](https://github.com/0xCCF4/zrb/pull/10))

### Other

- nixos noxa integration
- *(deps)* bump toml from 0.8.23 to 1.1.2+spec-1.1.0 ([#5](https://github.com/0xCCF4/zrb/pull/5))
- *(deps)* bump sd-notify from 0.4.5 to 0.5.0 ([#6](https://github.com/0xCCF4/zrb/pull/6))
- *(deps)* bump clap_mangen from 0.2.33 to 0.3.0 ([#7](https://github.com/0xCCF4/zrb/pull/7))

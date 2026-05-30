use std::path::PathBuf;
use std::process;

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use sd_notify::NotifyState;

use zrb::{config, ops};

#[derive(ValueEnum, Clone)]
enum ShellChoice {
    Bash,
    Zsh,
    Fish,
    Nushell,
    Elvish,
}

#[derive(Parser)]
#[command(name = "zrb", about = "ZFS remote backup tool", version)]
struct Cli {
    /// Enable debug logging.
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Override the default config file path.
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a zrb-managed snapshot of one or more datasets.
    Snapshot {
        /// Datasets to snapshot (e.g. tank/home).
        #[arg(required = true)]
        datasets: Vec<String>,
    },

    /// List zrb-managed snapshots. Omit DATASET to list all datasets.
    List {
        /// Dataset to inspect (omit to list all).
        dataset: Option<String>,

        /// Also list child datasets. Without DATASET, lists all datasets.
        #[arg(long, short = 'r')]
        recursive: bool,
    },

    /// Send snapshots to one or more configured Remotes.
    Send {
        /// Datasets to send (e.g. tank/home tank/documents). Omit to send all datasets from config.
        datasets: Vec<String>,

        /// Restrict send to a named Remote (repeatable).
        #[arg(long = "remote")]
        remotes: Vec<String>,

        /// Resume an interrupted transfer without creating a new snapshot.
        /// Errors if the newest local snapshot is already present on the Remote.
        #[arg(long)]
        resume: bool,

        /// Send to each Remote one at a time instead of in parallel.
        #[arg(long)]
        sequential: bool,

        /// Activate the full-screen TUI (requires a controlling TTY).
        #[arg(long)]
        tui: bool,
    },

    /// Prune zrb-managed snapshots according to the Retention Policy.
    Prune {
        /// Datasets to prune. Omit to read the list from the config file.
        datasets: Vec<String>,

        /// Preview what would be pruned without deleting anything.
        #[arg(long, short='n')]
        dry_run: bool,

        /// Abort any in-progress resume transfer and prune anyway.
        #[arg(long)]
        abort_resume: bool,
    },

    /// Run in server mode (invoked via SSH `ForceCommand`).
    Server {
        /// Permitted client name(s) for this SSH key (repeatable).
        #[arg(long = "client", required = true)]
        clients: Vec<String>,
    },

    #[command(hide = true)]
    Completions { shell: ShellChoice },

    #[command(hide = true)]
    Man,
}

fn validate_dataset(ds: &str) -> anyhow::Result<()> {
    if ds.starts_with('/') {
        anyhow::bail!("invalid dataset \"{ds}\": ZFS dataset paths must not start with \"/\"");
    }
    Ok(())
}

fn xdg_config_home() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from(".config"))
}

fn default_source_config() -> PathBuf {
    xdg_config_home().join("zrb/config.toml")
}

fn default_server_config() -> PathBuf {
    xdg_config_home().join("zrb/server.toml")
}

enum DryRunEntry<'a> {
    Keep(&'a str, &'a zrb::retention::policy::KeepReason),
    Delete(&'a str),
}

fn print_prune_dry_run(groups: &[(String, ops::prune::PruneResult)]) {
    use zrb::snapshot::naming;

    for (i, (dataset, result)) in groups.iter().enumerate() {
        if i > 0 {
            println!();
        }
        println!("{dataset}");
        if result.resume_skipped {
            println!("  \u{23f8} skipped \u{2014} resume transfer in progress");
            continue;
        }
        let mut entries: Vec<(Option<chrono::DateTime<chrono::Utc>>, DryRunEntry<'_>)> = result
            .kept
            .iter()
            .map(|(s, r)| (naming::parse(s), DryRunEntry::Keep(s, r)))
            .chain(
                result
                    .deleted
                    .iter()
                    .map(|s| (naming::parse(s), DryRunEntry::Delete(s))),
            )
            .collect();
        entries.sort_by_key(|(ts, _)| *ts);
        for (_, entry) in &entries {
            match entry {
                DryRunEntry::Keep(name, reason) => {
                    println!("  \u{2713} {:<9} {name}", reason.to_string());
                }
                DryRunEntry::Delete(name) => println!("  \u{2717}           {name}"),
            }
        }
        // Deduplicate by snapshot name before printing held entries.
        let mut seen = std::collections::HashSet::new();
        for (snap, tag) in &result.hold_skipped {
            if seen.insert(snap) {
                println!("  \u{23f8} held       {snap} ({tag})");
            }
        }
    }
}

fn print_grouped(groups: &[(String, Vec<String>)]) {
    for (i, (dataset, snaps)) in groups.iter().enumerate() {
        if i > 0 {
            println!();
        }
        println!("{dataset}");
        for s in snaps {
            println!("  {s}");
        }
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if !matches!(cli.command, Commands::Completions { .. } | Commands::Man) {
        check_zfs_version()?;
    }

    let log_level = if cli.verbose { "debug" } else { "warn" };
    zrb::tui::init_logger(log_level);

    match cli.command {
        Commands::Snapshot { datasets } => {
            for ds in &datasets {
                validate_dataset(ds)?;
            }
            let cfg_path = cli.config.unwrap_or_else(default_source_config);
            let cfg = config::load_source(&cfg_path)?;
            for ds in &datasets {
                let name = ops::snapshot::snapshot(ds, &cfg)?;
                log::info!("created {name}");
            }
        }

        Commands::List { dataset, recursive } => {
            if let Some(ds) = &dataset {
                validate_dataset(ds)?;
            }
            let groups: Vec<(String, Vec<String>)> = match dataset.as_deref() {
                None => ops::list::list_all()?,
                Some(ds) if recursive => ops::list::list_recursive(ds)?,
                Some(ds) => vec![(ds.to_owned(), ops::list::list(ds)?)],
            };
            print_grouped(&groups);
        }

        Commands::Send {
            datasets,
            remotes,
            resume,
            sequential,
            tui,
        } => {
            for ds in &datasets {
                validate_dataset(ds)?;
            }
            let cfg_path = cli.config.unwrap_or_else(default_source_config);
            let cfg = config::load_source(&cfg_path)?;
            let _ = sd_notify::notify(&[NotifyState::Ready]);
            let datasets = resolve_datasets(datasets, &cfg);
            let ds_refs: Vec<&str> = datasets.iter().map(String::as_str).collect();
            let filter: Option<Vec<&str>> = if remotes.is_empty() {
                None
            } else {
                Some(remotes.iter().map(String::as_str).collect())
            };

            // Safety: `isatty` is always safe to call with a valid fd.
            let maybe_tui = if tui {
                if unsafe { libc::isatty(libc::STDOUT_FILENO) } == 0 {
                    anyhow::bail!(
                        "--tui requires a controlling TTY; \
                         stdout is not a terminal (piped or redirected)"
                    );
                }

                // Build the dataset → remote-name display list for the countdown screen.
                let display_info: Vec<(String, Vec<String>)> = ds_refs
                    .iter()
                    .map(|&ds| {
                        let mut remote_names: Vec<String> = cfg
                            .datasets
                            .get(ds)
                            .into_iter()
                            .flat_map(|rt| {
                                rt.keys().filter(|n| {
                                    filter.as_deref().is_none_or(|f| f.contains(&n.as_str()))
                                })
                            })
                            .cloned()
                            .collect();
                        remote_names.sort_unstable();
                        (ds.to_owned(), remote_names)
                    })
                    .collect();

                let proceed = zrb::tui::run_countdown(&display_info).await?;
                if !proceed {
                    return Ok(());
                }

                let mut row_keys: Vec<String> = display_info
                    .iter()
                    .flat_map(|(ds, remotes)| {
                        remotes.iter().map(move |r| format!("{ds} \u{2192} {r}"))
                    })
                    .collect();
                row_keys.sort_unstable();

                let cancel_map = zrb::tui::build_cancel_map(&row_keys);
                let cancel_for_send: std::collections::HashMap<_, _> = cancel_map
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                let (tx, rx) = tokio::sync::mpsc::channel(256);
                let tui_task =
                    tokio::spawn(zrb::tui::run_transfer(rx, row_keys, cancel_map));

                Some((tx, tui_task, cancel_for_send))
            } else {
                None
            };

            let (event_tx, tui_task, cancel_map) = match maybe_tui {
                Some((tx, task, cancel)) => (Some(tx), Some(task), Some(cancel)),
                None => (None, None, None),
            };

            if resume {
                ops::send::send_resume(
                    &ds_refs, filter.as_deref(), &cfg, sequential, event_tx, cancel_map,
                )
                .await?;
            } else {
                ops::send::send(
                    &ds_refs, filter.as_deref(), &cfg, sequential, event_tx, cancel_map,
                )
                .await?;
            }

            if let Some(task) = tui_task {
                task.await??;
            }

            let _ = sd_notify::notify(&[NotifyState::Stopping]);
        }

        Commands::Prune {
            datasets,
            dry_run,
            abort_resume,
        } => {
            for ds in &datasets {
                validate_dataset(ds)?;
            }
            let cfg_path = cli.config.unwrap_or_else(default_source_config);
            // Server config takes priority: the remote runs `zrb prune` with
            // `--config server.toml`, which has `resume_hold_days`.
            let (retention, hold_days, config_datasets) = config::load_server(&cfg_path)
                .map(|c| {
                    let days = c.resume_hold_days();
                    let ds = c.configured_datasets();
                    (c.retention, Some(days), ds)
                })
                .or_else(|_| {
                    config::load_source(&cfg_path)
                        .map(|c| {
                            let ds = c.configured_datasets();
                            (c.retention, None, ds)
                        })
                })?;
            let targets = if datasets.is_empty() {
                config_datasets
            } else {
                datasets
            };
            let mut results: Vec<(String, ops::prune::PruneResult)> = Vec::new();
            for ds in targets {
                let result =
                    ops::prune::prune(&ds, &retention, hold_days, dry_run, abort_resume)?;
                results.push((ds, result));
            }
            if dry_run {
                print_prune_dry_run(&results);
            } else {
                for (ds, result) in &results {
                    let held: std::collections::HashSet<&str> = result
                        .hold_skipped
                        .iter()
                        .map(|(s, _)| s.as_str())
                        .collect();
                    log::info!(
                        "pruned {}: kept {}, deleted {}, held {}",
                        ds,
                        result.kept.len(),
                        result.deleted.len(),
                        held.len(),
                    );
                    for s in &result.deleted {
                        log::debug!("deleted {s}");
                    }
                    for s in &held {
                        log::debug!("held (Transfer Hold) {s}");
                    }
                }
            }
        }

        Commands::Server { clients } => {
            let cfg_path = cli.config.unwrap_or_else(default_server_config);
            let cfg = config::load_server(&cfg_path)?;
            ops::server::server(&cfg, &clients).await?;
        }

        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            let name = cmd.get_name().to_owned();
            let stdout = &mut std::io::stdout();
            match shell {
                ShellChoice::Bash => {
                    clap_complete::generate(clap_complete::Shell::Bash, &mut cmd, name, stdout);
                }
                ShellChoice::Zsh => {
                    clap_complete::generate(clap_complete::Shell::Zsh, &mut cmd, name, stdout);
                }
                ShellChoice::Fish => {
                    clap_complete::generate(clap_complete::Shell::Fish, &mut cmd, name, stdout);
                }
                ShellChoice::Elvish => {
                    clap_complete::generate(clap_complete::Shell::Elvish, &mut cmd, name, stdout);
                }
                ShellChoice::Nushell => {
                    clap_complete::generate(clap_complete_nushell::Nushell, &mut cmd, name, stdout);
                }
            }
        }

        Commands::Man => {
            let cmd = Cli::command();
            let man = clap_mangen::Man::new(cmd);
            let mut buf = Vec::new();
            man.render(&mut buf)?;
            std::io::Write::write_all(&mut std::io::stdout(), &buf)?;
        }
    }

    Ok(())
}

fn parse_zfs_version_line(line: &str) -> Result<(u32, u32, u32), String> {
    let rest = line
        .strip_prefix("zfs-")
        .ok_or_else(|| format!("unexpected zfs version format: {line}"))?;
    let semver = rest.split('-').next().unwrap_or(rest);
    let parts: Vec<&str> = semver.split('.').collect();
    if parts.len() < 3 {
        return Err(format!("unexpected zfs version format: {line}"));
    }
    let parse = |s: &str| {
        s.parse::<u32>()
            .map_err(|_| format!("unexpected zfs version format: {line}"))
    };
    Ok((parse(parts[0])?, parse(parts[1])?, parse(parts[2])?))
}

fn check_zfs_version() -> anyhow::Result<()> {
    let output = std::process::Command::new("zfs")
        .arg("version")
        .output()
        .map_err(|e| anyhow::anyhow!("could not determine zfs version: {e}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let first_line = stdout.lines().next().unwrap_or("").trim();
    let (major, minor, _patch) = parse_zfs_version_line(first_line)
        .map_err(|e| anyhow::anyhow!("could not determine zfs version: {e}"))?;
    if (major, minor) < (2, 3) {
        anyhow::bail!(
            "zrb requires zfs >= 2.3.0, found {}",
            first_line.trim_start_matches("zfs-")
        );
    }
    Ok(())
}

fn resolve_datasets(explicit: Vec<String>, config: &config::SourceConfig) -> Vec<String> {
    if explicit.is_empty() {
        config.configured_datasets()
    } else {
        explicit
    }
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e:#}");
        process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate(shell: clap_complete::Shell) -> Vec<u8> {
        let mut cmd = Cli::command();
        let name = cmd.get_name().to_owned();
        let mut buf = Vec::new();
        clap_complete::generate(shell, &mut cmd, name, &mut buf);
        buf
    }

    fn generate_nushell() -> Vec<u8> {
        let mut cmd = Cli::command();
        let name = cmd.get_name().to_owned();
        let mut buf = Vec::new();
        clap_complete::generate(clap_complete_nushell::Nushell, &mut cmd, name, &mut buf);
        buf
    }

    #[test]
    fn completions_bash_non_empty() {
        assert!(!generate(clap_complete::Shell::Bash).is_empty());
    }

    #[test]
    fn completions_zsh_non_empty() {
        assert!(!generate(clap_complete::Shell::Zsh).is_empty());
    }

    #[test]
    fn completions_fish_non_empty() {
        assert!(!generate(clap_complete::Shell::Fish).is_empty());
    }

    #[test]
    fn completions_nushell_non_empty() {
        assert!(!generate_nushell().is_empty());
    }

    #[test]
    fn completions_elvish_non_empty() {
        assert!(!generate(clap_complete::Shell::Elvish).is_empty());
    }

    #[test]
    fn send_tui_flag_parses() {
        let cli = Cli::try_parse_from(["zrb", "send", "--tui", "tank/home"]);
        assert!(cli.is_ok(), "zrb send --tui <dataset> should parse");
        if let Ok(Cli {
            command: Commands::Send { tui, .. },
            ..
        }) = cli
        {
            assert!(tui, "--tui should be true");
        }
    }

    #[test]
    fn send_tui_flag_absent_defaults_false() {
        let cli = Cli::try_parse_from(["zrb", "send", "tank/home"]).unwrap();
        if let Commands::Send { tui, .. } = cli.command {
            assert!(!tui, "--tui should default to false");
        }
    }

    #[test]
    fn send_tui_flag_absent_from_prune() {
        let cli = Cli::try_parse_from(["zrb", "prune", "tank/home", "--tui"]);
        assert!(cli.is_err(), "--tui must not exist on the prune subcommand");
    }

    #[test]
    fn send_event_variants_accessible() {
        use zrb::tui::SendEvent;
        let _ = SendEvent::AllDone;
        let _ = SendEvent::RemoteStarted {
            remote: "r".into(),
            total_bytes: 0,
        };
        let _ = SendEvent::RemoteProgress {
            remote: "r".into(),
            bytes_sent: 0,
        };
        let _ = SendEvent::RemoteCompleted {
            remote: "r".into(),
            elapsed_secs: 0.0,
            bytes: 0,
        };
        let _ = SendEvent::RemoteFailed {
            remote: "r".into(),
            error: "e".into(),
        };
        let _ = SendEvent::RemoteSkipped { remote: "r".into() };
        let _ = SendEvent::CountdownTick { remaining_secs: 5 };
        let _ = SendEvent::CountdownAborted;
    }

    #[test]
    fn send_no_args_parses() {
        let cli = Cli::try_parse_from(["zrb", "send"]);
        assert!(cli.is_ok(), "zrb send with no dataset args should parse (defaults to all config datasets)");
        if let Ok(Cli { command: Commands::Send { datasets, .. }, .. }) = cli {
            assert!(datasets.is_empty(), "datasets should be empty before resolve_datasets");
        }
    }

    #[test]
    fn send_resume_flag_parses() {
        let cli = Cli::try_parse_from(["zrb", "send", "--resume", "tank/home"]);
        assert!(cli.is_ok(), "zrb send --resume <dataset> should parse");
        if let Ok(Cli {
            command: Commands::Send { resume, .. },
            ..
        }) = cli
        {
            assert!(resume, "--resume should be true");
        }
    }

    #[test]
    fn send_resume_flag_absent_defaults_false() {
        let cli = Cli::try_parse_from(["zrb", "send", "tank/home"]).unwrap();
        if let Commands::Send { resume, .. } = cli.command {
            assert!(!resume, "--resume should default to false");
        }
    }

    #[test]
    fn send_sequential_flag_parses() {
        let cli = Cli::try_parse_from(["zrb", "send", "--sequential", "tank/home"]);
        assert!(cli.is_ok(), "zrb send --sequential <dataset> should parse");
        if let Ok(Cli {
            command: Commands::Send { sequential, .. },
            ..
        }) = cli
        {
            assert!(sequential, "--sequential should be true");
        }
    }

    #[test]
    fn send_sequential_flag_absent_defaults_false() {
        let cli = Cli::try_parse_from(["zrb", "send", "tank/home"]).unwrap();
        if let Commands::Send { sequential, .. } = cli.command {
            assert!(!sequential, "--sequential should default to false");
        }
    }

    #[test]
    fn prune_no_args_parses() {
        let cli = Cli::try_parse_from(["zrb", "prune"]);
        assert!(cli.is_ok(), "zrb prune with no args should parse (auto-from-config)");
        if let Ok(Cli { command: Commands::Prune { datasets, .. }, .. }) = cli {
            assert!(datasets.is_empty());
        }
    }

    #[test]
    fn prune_single_dataset_parses() {
        let cli = Cli::try_parse_from(["zrb", "prune", "tank/home"]);
        assert!(cli.is_ok(), "zrb prune <dataset> should parse");
        if let Ok(Cli { command: Commands::Prune { datasets, .. }, .. }) = cli {
            assert_eq!(datasets, vec!["tank/home"]);
        }
    }

    #[test]
    fn prune_multiple_datasets_parse() {
        let cli = Cli::try_parse_from(["zrb", "prune", "tank/home", "tank/projects"]);
        assert!(cli.is_ok(), "zrb prune <d1> <d2> should parse");
        if let Ok(Cli { command: Commands::Prune { datasets, .. }, .. }) = cli {
            assert_eq!(datasets, vec!["tank/home", "tank/projects"]);
        }
    }

    #[test]
    fn prune_all_flag_is_error() {
        let cli = Cli::try_parse_from(["zrb", "prune", "--all"]);
        assert!(cli.is_err(), "--all must not exist on the prune subcommand");
    }

    #[test]
    fn prune_recursive_flag_is_error() {
        let cli = Cli::try_parse_from(["zrb", "prune", "tank", "--recursive"]);
        assert!(cli.is_err(), "--recursive must not exist on the prune subcommand");
    }


    #[test]
    fn list_no_args_parses() {
        let cli = Cli::try_parse_from(["zrb", "list"]);
        assert!(cli.is_ok(), "zrb list with no args should parse");
        if let Ok(Cli {
            command: Commands::List { dataset, recursive },
            ..
        }) = cli
        {
            assert!(dataset.is_none());
            assert!(!recursive);
        }
    }

    #[test]
    fn list_with_dataset_parses() {
        let cli = Cli::try_parse_from(["zrb", "list", "tank/home"]).unwrap();
        if let Commands::List { dataset, recursive } = cli.command {
            assert_eq!(dataset.as_deref(), Some("tank/home"));
            assert!(!recursive);
        }
    }

    #[test]
    fn list_recursive_with_dataset_parses() {
        let cli = Cli::try_parse_from(["zrb", "list", "tank", "--recursive"]).unwrap();
        if let Commands::List { dataset, recursive } = cli.command {
            assert_eq!(dataset.as_deref(), Some("tank"));
            assert!(recursive);
        }
    }

    #[test]
    fn list_recursive_short_flag_parses() {
        let cli = Cli::try_parse_from(["zrb", "list", "tank", "-r"]).unwrap();
        if let Commands::List { recursive, .. } = cli.command {
            assert!(recursive);
        }
    }

    #[test]
    fn list_recursive_without_dataset_parses() {
        let cli = Cli::try_parse_from(["zrb", "list", "--recursive"]);
        assert!(
            cli.is_ok(),
            "zrb list --recursive with no dataset should parse"
        );
    }


    #[test]
    fn validate_dataset_rejects_absolute_path() {
        let err = validate_dataset("/something").unwrap_err();
        assert!(err.to_string().contains("/something"));
    }

    #[test]
    fn validate_dataset_accepts_pool_slash_dataset() {
        assert!(validate_dataset("tank/home").is_ok());
    }

    #[test]
    fn validate_dataset_accepts_bare_pool() {
        assert!(validate_dataset("tank").is_ok());
    }

    #[test]
    fn prune_dry_run_flag_parses() {
        let cli = Cli::try_parse_from(["zrb", "prune", "tank/home", "--dry-run"]);
        assert!(cli.is_ok(), "zrb prune <dataset> --dry-run should parse");
        if let Ok(Cli {
            command: Commands::Prune { dry_run, .. },
            ..
        }) = cli
        {
            assert!(dry_run, "--dry-run should be true");
        }
    }

    #[test]
    fn prune_abort_resume_flag_parses() {
        let cli = Cli::try_parse_from(["zrb", "prune", "tank/home", "--abort-resume"]);
        assert!(
            cli.is_ok(),
            "zrb prune <dataset> --abort-resume should parse"
        );
        if let Ok(Cli {
            command: Commands::Prune { abort_resume, .. },
            ..
        }) = cli
        {
            assert!(abort_resume, "--abort-resume should be true");
        }
    }

    #[test]
    fn prune_abort_resume_defaults_false() {
        let cli = Cli::try_parse_from(["zrb", "prune", "tank/home"]).unwrap();
        if let Commands::Prune { abort_resume, .. } = cli.command {
            assert!(!abort_resume, "--abort-resume should default to false");
        }
    }

    #[test]
    fn prune_dry_run_defaults_false() {
        let cli = Cli::try_parse_from(["zrb", "prune", "tank/home"]).unwrap();
        if let Commands::Prune { dry_run, .. } = cli.command {
            assert!(!dry_run, "--dry-run should default to false");
        }
    }

    #[test]
    fn man_page_contains_th_header() {
        let cmd = Cli::command();
        let man = clap_mangen::Man::new(cmd);
        let mut buf = Vec::new();
        man.render(&mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains(".TH"), "man page should contain .TH roff header");
    }

    #[test]
    fn version_parse_exact_floor_accepted() {
        assert_eq!(parse_zfs_version_line("zfs-2.3.0-1").unwrap(), (2, 3, 0));
    }

    #[test]
    fn version_parse_below_floor_rejected() {
        let (major, minor, _) = parse_zfs_version_line("zfs-2.2.99-1").unwrap();
        assert!((major, minor) < (2, 3), "2.2.x should be below floor");
    }

    #[test]
    fn version_parse_patch_release_accepted() {
        assert_eq!(parse_zfs_version_line("zfs-2.3.7-1").unwrap(), (2, 3, 7));
    }

    #[test]
    fn version_parse_major_bump_accepted() {
        assert_eq!(parse_zfs_version_line("zfs-3.0.0-1").unwrap(), (3, 0, 0));
    }

    #[test]
    fn version_parse_ubuntu_build_tag_accepted() {
        assert_eq!(
            parse_zfs_version_line("zfs-2.3.7-0ubuntu1").unwrap(),
            (2, 3, 7)
        );
    }

    #[test]
    fn version_parse_ubuntu_old_rejected() {
        let (major, minor, _) =
            parse_zfs_version_line("zfs-2.2.2-0ubuntu9.4").unwrap();
        assert!((major, minor) < (2, 3));
    }

    #[test]
    fn version_parse_malformed_is_err() {
        assert!(parse_zfs_version_line("not-a-version").is_err());
        assert!(parse_zfs_version_line("zfs-bad").is_err());
    }

    const RESOLVE_CFG_TOML: &str = r#"
[source]
name = "laptop"

[remotes.primary]
host = "backup.example.com"

[datasets."tank/home"]
primary = "backup/home"

[datasets."tank/documents"]
primary = "backup/documents"

[retention]
recent = 7
weekly_for_days = 30
monthly_for_days = 365
"#;

    #[test]
    fn resolve_datasets_empty_returns_all_config_datasets() {
        let cfg: config::SourceConfig = toml::from_str(RESOLVE_CFG_TOML).expect("should parse");
        let result = resolve_datasets(vec![], &cfg);
        assert_eq!(result, vec!["tank/documents", "tank/home"]);
    }

    #[test]
    fn resolve_datasets_explicit_passthrough() {
        let cfg: config::SourceConfig = toml::from_str(RESOLVE_CFG_TOML).expect("should parse");
        let result = resolve_datasets(vec!["tank/home".to_owned()], &cfg);
        assert_eq!(result, vec!["tank/home"]);
    }
}

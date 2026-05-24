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
#[command(name = "zrb", about = "ZFS remote backup tool")]
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

    /// List zrb-managed snapshots for a dataset.
    List {
        /// Dataset to inspect.
        dataset: String,
    },

    /// Send snapshots to one or more configured Remotes.
    Send {
        /// Datasets to send (e.g. tank/home tank/documents).
        #[arg(required = true)]
        datasets: Vec<String>,

        /// Restrict send to a named Remote (repeatable).
        #[arg(long = "remote")]
        remotes: Vec<String>,

        /// Resume an interrupted transfer without creating a new snapshot.
        /// Errors if the newest local snapshot is already present on the Remote.
        #[arg(long)]
        resume: bool,
    },

    /// Prune zrb-managed snapshots according to the Retention Policy.
    Prune {
        /// Dataset to prune (mutually exclusive with --all).
        #[arg(conflicts_with = "all", required_unless_present = "all")]
        dataset: Option<String>,

        /// Prune all datasets that have zrb-managed snapshots.
        #[arg(long, conflicts_with = "dataset")]
        all: bool,
    },

    /// Run in server mode (invoked via SSH `ForceCommand`).
    Server {
        /// Permitted client name(s) for this SSH key (repeatable).
        #[arg(long = "client", required = true)]
        clients: Vec<String>,
    },

    #[command(hide = true)]
    Completions {
        shell: ShellChoice,
    },

    #[command(hide = true)]
    Man,
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

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let log_level = if cli.verbose { "debug" } else { "info" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();

    match cli.command {
        Commands::Snapshot { datasets } => {
            let cfg_path = cli.config.unwrap_or_else(default_source_config);
            let cfg = config::load_source(&cfg_path)?;
            for ds in &datasets {
                let name = ops::snapshot::snapshot(ds, &cfg)?;
                log::info!("created {name}");
            }
        }

        Commands::List { dataset } => {
            let snaps = ops::list::list(&dataset)?;
            for s in snaps {
                println!("{s}");
            }
        }

        Commands::Send { datasets, remotes, resume } => {
            let cfg_path = cli.config.unwrap_or_else(default_source_config);
            let cfg = config::load_source(&cfg_path)?;
            let _ = sd_notify::notify(&[NotifyState::Ready]);
            let ds_refs: Vec<&str> = datasets.iter().map(String::as_str).collect();
            let filter: Option<Vec<&str>> = if remotes.is_empty() {
                None
            } else {
                Some(remotes.iter().map(String::as_str).collect())
            };
            if resume {
                ops::send::send_resume(&ds_refs, filter.as_deref(), &cfg)?;
            } else {
                ops::send::send(&ds_refs, filter.as_deref(), &cfg)?;
            }
            let _ = sd_notify::notify(&[NotifyState::Stopping]);
        }

        Commands::Prune { dataset, all } => {
            let cfg_path = cli.config.unwrap_or_else(default_source_config);
            // Server config takes priority: the remote runs `zrb prune` with
            // `--config server.toml`, which has `resume_hold_days`.
            let (retention, hold_days) = config::load_server(&cfg_path)
                .map(|c| {
                    let days = c.resume_hold_days();
                    (c.retention, Some(days))
                })
                .or_else(|_| config::load_source(&cfg_path).map(|c| (c.retention, None)))?;
            if all {
                let results = ops::prune::prune_all(&retention, hold_days)?;
                for (ds, result) in &results {
                    log::info!(
                        "pruned {}: kept {}, deleted {}",
                        ds,
                        result.kept.len(),
                        result.deleted.len()
                    );
                    for s in &result.deleted {
                        log::debug!("deleted {s}");
                    }
                }
            } else {
                let dataset = dataset.expect("required_unless_present = all");
                let result = ops::prune::prune(&dataset, &retention, hold_days)?;
                log::info!(
                    "pruned {}: kept {}, deleted {}",
                    dataset,
                    result.kept.len(),
                    result.deleted.len()
                );
                for s in &result.deleted {
                    log::debug!("deleted {s}");
                }
            }
        }

        Commands::Server { clients } => {
            let cfg_path = cli.config.unwrap_or_else(default_server_config);
            let cfg = config::load_server(&cfg_path)?;
            ops::server::server(&cfg, &clients)?;
        }

        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            let name = cmd.get_name().to_owned();
            let stdout = &mut std::io::stdout();
            match shell {
                ShellChoice::Bash => clap_complete::generate(clap_complete::Shell::Bash, &mut cmd, name, stdout),
                ShellChoice::Zsh => clap_complete::generate(clap_complete::Shell::Zsh, &mut cmd, name, stdout),
                ShellChoice::Fish => clap_complete::generate(clap_complete::Shell::Fish, &mut cmd, name, stdout),
                ShellChoice::Elvish => clap_complete::generate(clap_complete::Shell::Elvish, &mut cmd, name, stdout),
                ShellChoice::Nushell => clap_complete::generate(clap_complete_nushell::Nushell, &mut cmd, name, stdout),
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
    fn send_resume_flag_parses() {
        let cli = Cli::try_parse_from(["zrb", "send", "--resume", "tank/home"]);
        assert!(cli.is_ok(), "zrb send --resume <dataset> should parse");
        if let Ok(Cli { command: Commands::Send { resume, .. }, .. }) = cli {
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
    fn prune_all_flag_parses() {
        let cli = Cli::try_parse_from(["zrb", "prune", "--all"]);
        assert!(cli.is_ok(), "zrb prune --all should parse successfully");
    }

    #[test]
    fn prune_dataset_alone_parses() {
        let cli = Cli::try_parse_from(["zrb", "prune", "tank/home"]);
        assert!(cli.is_ok(), "zrb prune <dataset> should still parse successfully");
    }

    #[test]
    fn prune_no_args_errors() {
        let cli = Cli::try_parse_from(["zrb", "prune"]);
        assert!(cli.is_err(), "zrb prune with no args should be a CLI error");
    }

    #[test]
    fn prune_dataset_and_all_conflict() {
        let cli = Cli::try_parse_from(["zrb", "prune", "tank/home", "--all"]);
        assert!(cli.is_err(), "zrb prune <dataset> --all should be a CLI error");
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
}

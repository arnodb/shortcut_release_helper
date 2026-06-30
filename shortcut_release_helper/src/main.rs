//! An utility to find all Shortcut stories for a future release.
//!
//! This tool, given a list of repository and, for each repository, a **release** branch and a
//! **next** branch, finds all commits only present in the **next** branch. It then attempts to
//! locate [Shortcut](https://shortcut.com/) stories linked to each commit, as well as any epic
//! these stories may belong to. Finally, it produces a Markdown release notes file based on a
//! template.
//!
//! # Usage
//!
//! ```bash
//! $ ./shortcut_release_helper \
//!     --version 3.4.0 \
//!     --name 'Super release' \
//!     --description 'Exciting release' \
//!     notes.md
//! ```
//!
//! # Configuration
//!
//! This tool expects a `config.toml`, in the current working directory, like so:
//!
//! ```toml
//! template_file = "template.md.jinja"
//!
//! [repositories]
//! # Name of the first repository, can be anything
//! dev = { location = "../project1", release_branch = "master", next_branch = "next" }
//! # Same for the second repository
//! legacy = { location = "../project2", release_branch = "master", next_branch = "next" }
//! ```
//!
//! # Debugging
//!
//! You can use `RUST_LOG` to control the amount logged by the utility in the console.

#[macro_use]
extern crate derive_more;

#[macro_use]
extern crate static_assertions;

use std::{
    collections::HashMap,
    env::{var, VarError},
    fs,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    thread::JoinHandle,
    time::Duration,
};

use ansi_term::{
    Colour::{Blue, Green, Red},
    Style,
};
use anyhow::{anyhow, Result};
use clap::Parser;
use governor::{state::StreamRateLimitExt, Quota, RateLimiter};
use quirky_binder_capnp::run_chain;
use quirky_binder_support::prelude::ChainConfiguration;
use scoped_tls::scoped_thread_local;
use serde::Serialize;
use shortcut::StoryId;
use shortcut_client::apis::configuration as shortcut_cfg;
use shortcut_client::apis::default_api as shortcut_api;
use shortcut_client::models::{Epic, Story};
use tokio::{runtime::Handle, sync::Mutex};
use tokio_stream::wrappers::ReceiverStream;
use types::{RepoToCommits, RepoToHeadCommit};

use crate::{
    config::AppConfig,
    shortcut::{run_streamed_tasks, ItemRetriever},
    types::ShortcutApiKey,
};

mod config;
mod shortcut;
mod template;
mod types;

/// A command-line tool to generate release notes.
#[derive(Parser, Debug)]
#[clap(author, about, long_about = None, disable_version_flag = true)]
struct Args {
    /// Output file for the release notes
    output_file: PathBuf,
    /// Version to release
    #[clap(long)]
    version: Option<String>,
    /// Name of the release
    #[clap(long)]
    name: Option<String>,
    /// Description of the release
    #[clap(long)]
    description: Option<String>,
    /// Id of story to exclude, can be used multiple times
    #[clap(long)]
    exclude_story_id: Vec<StoryId>,
    /// Label of story to exclude, can be used multiple times - has priority over
    /// include-story-label if a story is tagged multiple times
    #[clap(long)]
    exclude_story_label: Vec<String>,
    /// Label of story to include, can be used multiple times
    #[clap(long)]
    include_story_label: Vec<String>,
    /// Exclude unparsed commits
    #[clap(long)]
    exclude_unparsed_commits: bool,
}

fn print_summary(release: &Release) {
    let header_style = Style::new().bold();
    println!(
        "{}: {}",
        header_style.paint("Total stories"),
        Green.paint(release.stories.len().to_string())
    );
    println!(
        "\n{}: {}",
        header_style.paint("Total epics"),
        Green.paint(release.epics.len().to_string())
    );
    for (repo, commits) in &release.unparsed_commits {
        if !commits.is_empty() {
            println!(
                "\n{}{}: {}",
                header_style.paint("Total unparsed commits in "),
                Blue.paint(repo.as_ref()),
                Red.paint(commits.len().to_string())
            );
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Release {
    pub name: Option<String>,
    pub version: Option<String>,
    pub description: Option<String>,
    pub stories: Vec<Story>,
    pub epics: Vec<Epic>,
    pub unparsed_commits: RepoToCommits,
    pub next_heads: RepoToHeadCommit,
}

#[allow(dead_code)]
#[allow(clippy::borrowed_box)]
#[allow(clippy::module_inception)]
mod chain {
    include!(concat!(env!("OUT_DIR"), "/chain.rs"));
}

#[derive(Clone)]
struct QuirkyShared {
    args: Arc<Args>,
    config: Arc<AppConfig>,
    shortcut_story_retriever: Arc<DynItemRetriever<Story, i64>>,
    shortcut_epic_retriever: Arc<DynItemRetriever<Epic, i64>>,
    output: Arc<Mutex<Release>>,
}

scoped_thread_local!(static QUIRKY_SHARED: QuirkyShared);

type DynItemRetriever<Item, Id> = ItemRetriever<
    Item,
    Id,
    Box<
        dyn Fn(Id) -> Pin<Box<dyn Future<Output = Result<Item, anyhow::Error>> + Send>>
            + Send
            + Sync,
    >,
    Pin<Box<dyn Future<Output = Result<Item, anyhow::Error>> + Send>>,
>;

fn custom_spawn<F, T>(f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    let handle = Handle::current();
    QUIRKY_SHARED.with(|shared| {
        let shared = shared.clone();
        std::thread::spawn(move || {
            QUIRKY_SHARED.set(&shared, || {
                let _guard = handle.enter();
                f()
            })
        })
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    std::panic::set_hook(Box::new(|panic_info| {
        dbg!(panic_info);
        std::thread::sleep(Duration::from_mins(60));
    }));

    let _ = dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();
    let args = Arc::new(Args::parse());
    let api_key = ShortcutApiKey::new(var("SHORTCUT_TOKEN").map_err(|err| match err {
        VarError::NotPresent => anyhow!("Missing SHORTCUT_TOKEN environment variable. Please provide it in a .env file or set it in your environment."),
        VarError::NotUnicode(_) => err.into(),
    })?);
    let config = Arc::new(AppConfig::parse(&PathBuf::from("config.toml"))?);
    let template_content = fs::read_to_string(&config.template_file)?;
    let template = template::FileTemplate::new(&template_content)?;

    let mut chain_configuration = ChainConfiguration::new();

    chain_configuration
        .variables
        .insert("shortcut_api_key".to_owned(), api_key.as_ref().clone());

    let mut configuration = shortcut_cfg::Configuration::new();
    configuration.api_key = Some(shortcut_cfg::ApiKey {
        key: api_key.as_ref().clone(),
        prefix: None,
    });
    let configuration = Arc::new(configuration);

    let (shortcut_story_retriever, shortcut_epic_retriever, join_shortcut_retriever) = {
        let (sender, receiver) = tokio::sync::mpsc::channel(1000);
        let story_retriever = ItemRetriever::new(
            {
                let configuration = configuration.clone();
                Box::new(move |id| {
                    let configuration = configuration.clone();
                    Box::pin(async move {
                        shortcut_api::get_story(&configuration, id)
                            .await
                            .map_err(|err| anyhow::anyhow!("{err}"))
                    }) as Pin<Box<dyn Future<Output = _> + Send>>
                })
                    as Box<dyn Fn(_) -> Pin<Box<dyn Future<Output = _> + Send>> + Send + Sync>
            },
            sender.clone(),
        );
        let epic_retriever = ItemRetriever::new(
            {
                let configuration = configuration.clone();
                Box::new(move |id| {
                    let configuration = configuration.clone();
                    Box::pin(async move {
                        shortcut_api::get_epic(&configuration, id)
                            .await
                            .map_err(|err| anyhow::anyhow!("{err}"))
                    }) as Pin<Box<dyn Future<Output = _> + Send>>
                })
                    as Box<dyn Fn(_) -> Pin<Box<dyn Future<Output = _> + Send>> + Send + Sync>
            },
            sender.clone(),
        );
        let join_retriever = Handle::current().spawn(async move {
            let rate_limiter = RateLimiter::direct(Quota::per_minute(1000.try_into().unwrap()));
            let task_stream = ReceiverStream::new(receiver).ratelimit_stream(&rate_limiter);
            run_streamed_tasks(task_stream).await;
        });
        (
            Arc::new(story_retriever),
            Arc::new(epic_retriever),
            join_retriever,
        )
    };

    let output = Arc::new(Mutex::new(Release {
        name: args.name.clone(),
        version: args.version.clone(),
        description: args.description.clone(),
        stories: Vec::new(),
        epics: Vec::new(),
        unparsed_commits: HashMap::new(),
        next_heads: HashMap::new(),
    }));

    let (chain_status, join) = QUIRKY_SHARED.set(
        &QuirkyShared {
            args: args.clone(),
            config,
            shortcut_story_retriever,
            shortcut_epic_retriever,
            output: output.clone(),
        },
        || chain::main(chain_configuration).unwrap(),
    );
    run_chain(chain_status, || join.join_all()).unwrap();

    join_shortcut_retriever.await.unwrap();

    let release = Arc::into_inner(output).unwrap().into_inner();
    print_summary(&release);
    template.render_to_file(&release, &args.output_file)?;
    Ok(())
}

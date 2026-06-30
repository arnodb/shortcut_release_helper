use std::collections::BTreeMap;
use std::path::Path;

use quirky_binder::{prelude::*, quirky_binder};
use truc::record::type_resolver::StaticTypeResolver;

pub fn main() {
    quirky_binder!(inline(
        r###"
use quirky_binder::{
    filter::{
        dedup::dedup,
        fork::extract_fields::extract_fields,
        function::produce::function_produce,
        function::terminate::function_terminate,
        function::update::function_update,
        group::group,
        pipe::pipe,
        sort::sort,
        unwrap::unwrap,
    },
};

{
  (
      function_produce#read_repositories(
        fields: [
            ("repo_name", "String"),
            ("id", "String"),
            ("message", "Option<String>"),
        ],
        body: r#"
            use anyhow::Context;
            use git2::Repository;

            use crate::types::HeadCommit;

            let mut total_count = 0;

            let (repositories, out) = crate::QUIRKY_SHARED.with(|shared| {
                (
                    shared.config.repositories.clone(),
                    shared.output.clone(),
                )
            });
            for (repo_name, repo_config) in repositories {
                let repository = Repository::open(repo_config.location.as_ref())
                    .with_context(|| format!("Could not open repository {} at {}", repo_name, repo_config.location.as_ref().to_string_lossy()))?;

                let mut revwalk = repository.revwalk()?;
                revwalk.push(repository.revparse_single(repo_config.next_branch.as_ref())?.id())?;
                revwalk.hide(repository.revparse_single(repo_config.release_branch.as_ref())?.id())?;

                let mut count = 0;

                for rev in revwalk {
                    let commit_id = rev?;
                    let commit = repository.find_commit(commit_id)?;
                    let message = commit.message().map(str::to_owned);
                    let record = Output0::new(UnpackedOutput0 {
                        repo_name: repo_name.as_ref().to_owned(),
                        id: commit.id().to_string(),
                        message: message.clone(),
                    });
                    if count == 0 {
                        let handle = tokio::runtime::Handle::current();
                        handle.block_on(async {
                            let mut release = out.lock().await;
                            release.next_heads.insert(repo_name.clone(), HeadCommit {
                                id: commit_id,
                                message,
                            });
                        });
                    }
                    count +=1;
                    output.send(Some(record))?;
                }

                println!("Found {} commits in repo {}", count, repo_name);

                total_count += count;
            }
            
            println!("Found a total of {} commits", total_count);

            output.send(None)?;

            Ok(())
"#,
      )
    - function_update#parse_story_id(
        add_fields: [("story_id", "Option<i64>")],
        body: r#"
            use std::collections::HashMap;
            use std::str::FromStr;
            use std::sync::{Arc, LazyLock, Mutex};

            use anyhow::anyhow;
            use git2::Oid;
            use regex::Regex;

            use crate::types::{RepositoryName, UnreleasedCommit};

            static SHORTCUT_RE: LazyLock<Regex> = LazyLock::new(||
                Regex::new(r"(?:(\[|/)sc-|(\[|/)ch|story/)(\d+)")
                    .expect("Could not compile SHORTCUT_RE")
            );

            let unparsed_commits = Arc::<Mutex::<HashMap::<RepositoryName, Vec<UnreleasedCommit>>>>::default();
            let unparsed_commits2 = unparsed_commits.clone();

            input
                .map(move |record| {
                    let UnpackedInput0 { repo_name, id, message } = record.unpack();

                    let story_id = message.as_ref()
                        .and_then(|message| {
                            SHORTCUT_RE.captures(message).map(|captures| {
                                captures
                                    .get(3)
                                    .expect("Story id should be captured")
                                    .as_str()
                            })
                        })
                        .map(|story_id| i64::from_str(story_id).expect("Should be parsed as number"));

                    if story_id.is_none() {
                        unparsed_commits
                            .lock()
                            .map_err(|err| anyhow!("{err}"))?
                            .entry(repo_name.clone().into())
                            .or_default()
                            .push(UnreleasedCommit {
                                id: Oid::from_str(&id).unwrap(),
                                message: message.clone(),
                            });
                    }

                    Ok((repo_name, id, message, story_id).into())
                })
                .chain(fallible_iterator::from_fn(move || {
                    let unparsed_commits = std::mem::take(&mut *unparsed_commits2.lock().map_err(|err| anyhow!("{err}"))?);
                    crate::QUIRKY_SHARED.with(|shared| {
                        let handle = tokio::runtime::Handle::current();
                        handle.block_on(async {
                            let mut release = shared.output.lock().await;
                            release.unparsed_commits = unparsed_commits;
                        });
                    });
                    Ok(None)
                }))
"#,
      )
    - sort#sort_story_id(fields: ["story_id"])
    - group#group_story_id(by_fields: ["story_id"], group_field: "commits")
    - extract_fields#extract_story_id(fields: ["story_id"]) [story_ids]
    - function_terminate#stop(
        body: r#"
            //println!("=== Walk");
            while let Some(_record) = input.next()? {
            /*
                println!("--- Story {:?}", record.story_id());
                println!("Commits: [");
                for commit in record.commits() {
                    println!("    Id: {}", commit.id());
                    println!("    Message: {}", commit.message().as_deref().unwrap_or(""));
                }
                println!("]");
            */
            }
            //println!("=== End walk");
            Ok(())
"#,
      )
  )

  ( < story_ids
    - unwrap#unwrap_story_id(fields: ["story_id"], skip_nones: true)
    - function_update#request_story(
        body: r#"
            let handle = tokio::runtime::Handle::current();
            let story_retriever = crate::QUIRKY_SHARED.with(|shared| {
                shared.shortcut_story_retriever.clone()
            });
            input.map(move |record| {
                let story_id = *record.story_id();
                handle.block_on( story_retriever.request(story_id))?;
                Ok(record)
            })
"#,
      )
    - pipe#pipe_()
    - function_update#resolve_story(
        add_fields: [
            ("story", "shortcut_client::models::story::Story"),
            ("epic_id", "Option<i64>"),
        ],
        body: r#"
            use crate::shortcut::{StoryId, StoryLabelFilter};

            let handle = tokio::runtime::Handle::current();
            let (
                exclude_story_id,
                exclude_story_label,
                include_story_label,
                story_retriever,
            ) = crate::QUIRKY_SHARED.with(|shared| {
                (
                    shared.args.exclude_story_id.clone(),
                    shared.args.exclude_story_label.clone(),
                    shared.args.include_story_label.clone(),
                    shared.shortcut_story_retriever.clone(),
                )
            });
            let story_label_filter = StoryLabelFilter::new(&exclude_story_label, &include_story_label);
            input.filter_map(move |record| {
                let story_id = *record.story_id();
                if !exclude_story_id.is_empty()
                    && exclude_story_id.contains(&StoryId::from(u32::try_from(story_id).unwrap()))
                {
                    return Ok(None);
                }
                let story = handle.block_on(async {
                    let story = story_retriever.wait_for(story_id).await?.map(|arc| (*arc).clone());
                    Ok::<_, anyhow::Error>(story)
                })?;
                if let Some(story) = story {
                    if !story_label_filter.is_empty()
                        && !story_label_filter.filter(&story)
                    {
                        return Ok(None);
                    }
                    let epic_id = story.epic_id;
                    Ok(Some((record, (story, epic_id)).into()))
                } else {
                    eprintln!("Story {} not found", story_id);
                    Ok(None)
                }
            })
"#,
      )
    - extract_fields#extract_epic_id(fields: ["epic_id"]) [epic_ids]
    - function_terminate#collect_stories(
        body: r#"
            let mut stories = Vec::new();
            while let Some(record) = input.next()? {
                let UnpackedInput0 { story_id: _, story, epic_id: _ } = record.unpack();
                stories.push(story);
            }
            crate::QUIRKY_SHARED.with(|shared| {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(async {
                    let mut release = shared.output.lock().await;
                    release.stories = stories;
                });
            });
            Ok(())
"#,
      )
  )

  ( < epic_ids
    - unwrap#unwrap_epic_id(fields: ["epic_id"], skip_nones: true)
    - sort#sort_epic_id(fields: ["epic_id"])
    - dedup#dedup_epic_id()
    - function_update#request_epic(
        body: r#"
            let handle = tokio::runtime::Handle::current();
            let epic_retriever = crate::QUIRKY_SHARED.with(|shared| {
                shared.shortcut_epic_retriever.clone()
            });
            input.map(move |record| {
                let epic_id = *record.epic_id();
                handle.block_on(epic_retriever.request(epic_id))?;
                Ok(record)
            })
"#,
      )
    - pipe#pipe__()
    - function_update#resolve_epic(
        add_fields: [
            ("epic", "shortcut_client::models::epic::Epic"),
        ],
        body: r#"
            let handle = tokio::runtime::Handle::current();
            let epic_retriever = crate::QUIRKY_SHARED.with(|shared| {
                shared.shortcut_epic_retriever.clone()
            });
            input.map(move |record| {
                let epic_id = *record.epic_id();
                let epic = handle.block_on(async {
                    let epic = epic_retriever.wait_for(epic_id).await?.map(|arc| (*arc).clone());
                    Ok::<_, anyhow::Error>(epic)
                })?;
                let epic = if let Some(epic) = epic {
                    epic
                } else {
                    anyhow::bail!("Epic {} not found", epic_id);
                };
                Ok((record, epic).into())
            })
"#,
      )
    - function_terminate#collect_epics(
        body: r#"
            let mut epics = Vec::new();
            while let Some(record) = input.next()? {
                let UnpackedInput0 { epic_id: _, epic } = record.unpack();
                epics.push(epic);
            }
            crate::QUIRKY_SHARED.with(|shared| {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(async {
                    let mut release = shared.output.lock().await;
                    release.epics = epics;
                });
            });
            Ok(())
"#,
      )
  )
}
"###
    ));

    let type_resolver = {
        let mut resolver = StaticTypeResolver::new();
        resolver.add_all_types();
        resolver.add_type::<shortcut_client::models::epic::Epic>();
        resolver.add_type::<Option<shortcut_client::models::epic::Epic>>();
        resolver.add_type::<shortcut_client::models::story::Story>();
        resolver.add_type::<Option<shortcut_client::models::story::Story>>();
        resolver
    };

    let graph = quirky_binder_main(GraphBuilder::new(ChainCustomizer {
        threads: ThreadsCustomizer {
            spawn: "crate::custom_spawn".to_owned(),
        },
        ..Default::default()
    }))
    .unwrap_or_else(|err| {
        panic!("{}", err);
    });

    let out_dir = std::env::var("OUT_DIR").unwrap();
    graph.generate(Path::new(&out_dir), &type_resolver).unwrap();
}

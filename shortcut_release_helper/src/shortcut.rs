use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use derive_new::new;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use shortcut_client::models::Story;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct StoryLabelFilter {
    excluded_labels: HashSet<String>,
    included_labels: HashSet<String>,
}

impl<'a> StoryLabelFilter {
    pub fn new(excluded_labels: &'a [String], included_labels: &'a [String]) -> Self {
        Self {
            excluded_labels: HashSet::from_iter(excluded_labels.iter().cloned()),
            included_labels: HashSet::from_iter(included_labels.iter().cloned()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.excluded_labels.is_empty() && self.included_labels.is_empty()
    }

    pub fn filter(&self, story: &Story) -> bool {
        let mut included_labels_count = 0;
        for label in &story.labels {
            if self.excluded_labels.contains(&label.name) {
                return false;
            }
            if self.included_labels.contains(&label.name) {
                included_labels_count += 1;
            }
        }
        // This assumes that story labels, as returned by the API, are unique
        included_labels_count == self.included_labels.len()
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, AsRef, FromStr, Display, From, Into)]
pub struct StoryId(u32);

enum RetrievalState<T> {
    Requested(
        tokio::sync::watch::Sender<Option<Option<T>>>,
        tokio::sync::watch::Receiver<Option<Option<T>>>,
    ),
    Some(T),
    None,
    Error(anyhow::Error),
}

type RetrieverFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(new)]
pub struct ItemRetriever<Item, Id, Request, RequestFut>
where
    Item: Send + Sync + 'static,
    Id: Ord + Copy + Display + Send + Sync + 'static,
    Request: Fn(Id) -> RequestFut,
    RequestFut: Future<Output = Result<Item, anyhow::Error>> + Send + Unpin + 'static,
{
    #[new(default)]
    items: Arc<Mutex<BTreeMap<Id, RetrievalState<Arc<Item>>>>>,
    req: Request,
    sender: tokio::sync::mpsc::Sender<Box<dyn FnOnce() -> RetrieverFuture + Send>>,
}

impl<Item, Id, Request, RequestFut> ItemRetriever<Item, Id, Request, RequestFut>
where
    Item: Send + Sync + 'static,
    Id: Ord + Copy + Display + Send + Sync + 'static,
    Request: Fn(Id) -> RequestFut,
    RequestFut: Future<Output = Result<Item, anyhow::Error>> + Send + Unpin + 'static,
{
    pub async fn request(&self, id: Id) -> Result<Option<Option<Arc<Item>>>, anyhow::Error> {
        let mut locked_items = self.items.lock().await;
        let item = match locked_items.entry(id) {
            Entry::Vacant(vacant) => {
                let req = (self.req)(id);
                let items = self.items.clone();
                match self
                    .sender
                    .send(Box::new(move || {
                        Box::pin(async move {
                            let item = req.await;
                            match item {
                                Ok(item) => {
                                    Self::item_received(&items, id, item).await;
                                }
                                Err(_err) => {
                                    Self::item_not_found(&items, id).await;
                                }
                            }
                        })
                    }))
                    .await
                {
                    Ok(()) => {}
                    Err(err) => {
                        let bail_err = err.to_string();
                        vacant.insert(RetrievalState::Error(anyhow::anyhow!("{err}")));
                        anyhow::bail!(bail_err);
                    }
                }
                let (sender, receiver) = tokio::sync::watch::channel(None);
                vacant.insert(RetrievalState::Requested(sender, receiver.clone()));
                let item = receiver.borrow().clone();
                item
            }
            Entry::Occupied(occupied) => match occupied.get() {
                RetrievalState::Requested(_, receiver) => receiver.borrow().clone(),
                RetrievalState::Some(item) => Some(Some(item.clone())),
                RetrievalState::None => Some(None),
                RetrievalState::Error(err) => {
                    anyhow::bail!(err.to_string())
                }
            },
        };
        Ok(item)
    }

    pub async fn wait_for(&self, id: Id) -> Result<Option<Arc<Item>>, anyhow::Error> {
        let mut receiver = {
            let mut locked_items = self.items.lock().await;
            match locked_items.entry(id) {
                Entry::Vacant(_) => anyhow::bail!("Item {} was never requested", id),
                Entry::Occupied(occupied) => match occupied.get() {
                    RetrievalState::Requested(_, receiver) => receiver.clone(),
                    RetrievalState::Some(item) => return Ok(Some(item.clone())),
                    RetrievalState::None => return Ok(None),
                    RetrievalState::Error(err) => anyhow::bail!(err.to_string()),
                },
            }
        };
        loop {
            if let Some(story) = receiver.borrow().as_ref() {
                return Ok(story.clone());
            }
            receiver.changed().await?;
        }
    }

    async fn item_received(
        items: &Mutex<BTreeMap<Id, RetrievalState<Arc<Item>>>>,
        id: Id,
        item: Item,
    ) {
        let mut locked_items = items.lock().await;
        let state = locked_items.get_mut(&id);
        if let Some(state) = state {
            match state {
                RetrievalState::Requested(sender, _) => {
                    let item = Arc::new(item);
                    if let Err(err) = sender.send(Some(Some(item.clone()))) {
                        eprintln!("Error: {err}");
                    }
                    *state = RetrievalState::Some(item);
                }
                RetrievalState::Some(_) | RetrievalState::None | RetrievalState::Error(_) => {
                    // Too late
                }
            }
        } else {
            unreachable!();
        }
    }

    async fn item_not_found(items: &Mutex<BTreeMap<Id, RetrievalState<Arc<Item>>>>, id: Id) {
        let mut locked_items = items.lock().await;
        let state = locked_items.get_mut(&id);
        if let Some(state) = state {
            match state {
                RetrievalState::Requested(sender, _) => {
                    if let Err(err) = sender.send(Some(None)) {
                        eprintln!("Error: {err}");
                    }

                    *state = RetrievalState::None;
                }
                RetrievalState::Some(_) | RetrievalState::None | RetrievalState::Error(_) => {
                    // Too late
                }
            }
        } else {
            unreachable!();
        }
    }
}

pub async fn run_streamed_tasks<S, F, Fut>(mut receiver: S)
where
    S: Stream<Item = F> + Unpin,
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()> + Send,
{
    let mut futures = FuturesUnordered::<Fut>::new();

    let mut closed = false;

    loop {
        futures::select! {
           fut = receiver.next().fuse() => {
               if let Some(fut) = fut {
                   futures.push(fut());
               } else {
                   closed = true;
               }
           }
           res = futures.next() => {
               match res {
                   Some(()) | None => {}
               }
           }
        }
        if futures.is_empty() && closed {
            break;
        }
    }
}

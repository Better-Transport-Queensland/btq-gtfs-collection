//! BRIDGE
//!
//! Handles briding data from gtfs-structures and protobufs (for realtime)
//! to the DB types defined in db/types.rs
use crate::{
    db::{
        self,
        types::{InsertDB, insert_many, insert_one, static_types::LastUpdate},
    },
    gtfs::StaticGtfs,
};
use anyhow::{Context, Result, anyhow};
use futures::future::try_join_all;
use rayon::prelude::*;
use sqlx::{Acquire, postgres::types::PgInterval};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, warn};

pub mod realtime_bridge;
pub mod static_bridge;

pub fn convert<T, U>(
    label: &'static str,
    items: Vec<T>,
    chunk_size: usize,
) -> UnboundedReceiver<Vec<U>>
where
    T: ToDB<U> + Send + Sync + Clone + 'static,
    U: Send + Sync + 'static,
{
    let (sender, receiver): (UnboundedSender<Vec<U>>, UnboundedReceiver<Vec<U>>) =
        tokio::sync::mpsc::unbounded_channel();

    tokio::task::spawn_blocking(move || {
        debug!(%label, "Starting DB Conversion.");
        for chunk in items.chunks(chunk_size) {
            let converted: Vec<_> = chunk
                .par_iter()
                .filter_map(|item| item.clone().to_db().ok())
                .collect();

            if sender.send(converted).is_err() {
                break;
            }
        }
        debug!(%label, "Finished DB Conversion.");
    });

    receiver
}

pub async fn stream_insert<T: InsertDB + Send + Sync + 'static>(
    label: &'static str,
    db: &db::Db,
    mut rx: UnboundedReceiver<Vec<T>>,
) -> Result<JoinHandle<Result<()>>> {
    let mut tx = db.0.begin().await?;
    Ok(tokio::task::spawn(async move {
        info!(%label, "Starting DB insert.");
        while let Some(item) = rx.recv().await {
            let mut sp = (&mut tx).begin().await?;
            if let Err(e) = insert_many(item, &mut sp).await {
                warn!(%label, "Failed to insert chunk: {e}");
                sp.rollback().await?;
            } else {
                sp.commit().await?;
            }
        }
        tx.commit().await?;
        info!(%label, "Finished DB insert.");
        Ok(())
    }))
}

pub trait ToDB<T>: Send {
    fn to_db(self) -> Result<T>;
}

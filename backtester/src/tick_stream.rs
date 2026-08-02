//! Parallel, order-preserving Parquet decode for the event loop.
//!
//! The tick loop is sequential in time — a backtest cannot process April
//! before March — but *decoding* is not. Profiling a full-dataset scan put
//! two thirds of the wall time in the reader, so this module moves that work
//! onto a pool of threads and hands the engine the same strictly ordered tick
//! stream it had before.
//!
//! The unit of work is one **row group**: a self-contained, contiguous slice
//! of one month file's rows (the real dataset writes 30 of them per file, ~1M
//! rows each), which can be decoded without reference to any other. Units are
//! numbered in file order and dealt round-robin to the workers, so worker `w`
//! holds units `w`, `w + threads`, `w + 2·threads`, … in that order. The
//! consumer then reads its workers' channels in the same rotation, which puts
//! the units back in file order without a reorder buffer or a sequence
//! number: each channel is FIFO, and the rotation is the deal in reverse.
//!
//! Two details the design has to get right:
//!
//! - **Ticks straddle boundaries.** A tick is every bar sharing one timestamp
//!   (~1600 of them across a wide universe), and nothing aligns that to a
//!   batch or a row group. Workers group what they see; the consumer holds
//!   back the last group of every chunk and merges it with the next one when
//!   the timestamps match, so a tick split across two row groups arrives whole.
//! - **Errors stay deterministic.** A worker that hits unsorted data reports
//!   it and stops; because the consumer reads units in file order, it always
//!   surfaces the *first* bad row in the dataset, not whichever thread noticed
//!   first.
//!
//! Memory is bounded by construction: workers block once their channel is
//! full, so a fast thread cannot run arbitrarily far ahead of a slow one.

use std::{
    path::PathBuf,
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc,
    },
    thread::{self, JoinHandle},
};

use crate::{
    data::{
        open_row_group_reader, row_group_count, ts_to_datetime, BarColumns, SubscriptionMask, Tick,
    },
    error::BacktestError,
    symbol::Symbol,
};

/// Chunks a worker may have queued ahead of the consumer. Two is enough to
/// keep a thread from idling between handoffs without letting it run far
/// enough ahead to matter for memory.
const CHANNEL_DEPTH: usize = 2;

/// Cap on auto-selected decode threads. Past this the run is waiting on the
/// disk, not on decode, and every extra thread is just more resident batches.
const MAX_AUTO_THREADS: usize = 8;

/// How many decode threads to use when the caller asks for the default.
pub fn default_threads() -> usize {
    thread::available_parallelism().map_or(1, |n| n.get()).clamp(1, MAX_AUTO_THREADS)
}

/// One row group of one file: what a worker decodes at a time.
struct Unit {
    path: Arc<PathBuf>,
    row_group: usize,
}

enum Message {
    /// A run of consecutive ticks in file order. The first and last may each
    /// be partial — see the straddle handling in [`TickStream::next_tick`].
    Ticks(Arc<PathBuf>, Vec<Tick>),
    /// The worker finished a unit; the consumer's rotation moves on.
    UnitDone,
    /// The worker ran out of units and exited normally. Distinguishing this
    /// from a dropped channel is what keeps a panicking worker from looking
    /// like the end of the data.
    Finished,
    Failed(Box<BacktestError>),
}

/// An ordered tick stream over a set of Parquet files, decoded in parallel.
///
/// Yields exactly what draining a [`TickReader`](crate::data::TickReader) over
/// the same files in order would yield. Dropping it stops the workers.
pub struct TickStream {
    /// One channel per worker, read strictly in rotation — that is what
    /// restores file order.
    inboxes: Vec<Receiver<Message>>,
    handles: Vec<JoinHandle<()>>,
    turn: usize,
    /// Groups decoded but not yet handed out, from the chunk in hand.
    buffered: std::vec::IntoIter<Tick>,
    /// The tick being assembled. Held back until a group with a different
    /// timestamp proves it complete, since the next chunk may continue it.
    pending: Option<Tick>,
    /// File the chunk in hand came from, for error reporting.
    source: Option<Arc<PathBuf>>,
    last_ts: Option<i64>,
    finished: bool,
}

impl TickStream {
    /// Stream `files` (already in chronological order) through `threads`
    /// decode threads. `threads` of 0 means [`default_threads`].
    pub fn new(
        files: &[PathBuf],
        subscribed: &SubscriptionMask,
        threads: usize,
    ) -> Result<Self, BacktestError> {
        let mut units = Vec::new();
        for path in files {
            let path = Arc::new(path.clone());
            for row_group in 0..row_group_count(&path)? {
                units.push(Unit { path: Arc::clone(&path), row_group });
            }
        }

        let threads = if threads == 0 { default_threads() } else { threads };
        let threads = threads.clamp(1, units.len().max(1));

        // Deal units round-robin, so reading the workers in the same rotation
        // reads the units in order.
        let mut queues: Vec<Vec<Unit>> = (0..threads).map(|_| Vec::new()).collect();
        for (i, unit) in units.into_iter().enumerate() {
            queues[i % threads].push(unit);
        }

        let subscribed = Arc::new(subscribed.clone());
        let mut inboxes = Vec::with_capacity(threads);
        let mut handles = Vec::with_capacity(threads);
        for queue in queues {
            let (tx, rx) = sync_channel(CHANNEL_DEPTH);
            let subscribed = Arc::clone(&subscribed);
            handles.push(thread::spawn(move || run_worker(queue, &subscribed, &tx)));
            inboxes.push(rx);
        }

        Ok(Self {
            inboxes,
            handles,
            turn: 0,
            buffered: Vec::new().into_iter(),
            pending: None,
            source: None,
            last_ts: None,
            finished: false,
        })
    }

    /// The next tick: every subscribed bar sharing one timestamp. `Ok(None)`
    /// once every file is exhausted.
    pub fn next_tick(&mut self) -> Result<Option<Tick>, BacktestError> {
        loop {
            let Some((ts, bars)) = self.next_group()? else {
                // End of stream: whatever is pending is the final tick.
                return Ok(self.pending.take());
            };

            if let Some(last) = self.last_ts {
                if ts < last {
                    return Err(self.out_of_order(ts, last));
                }
            }
            self.last_ts = Some(ts);

            match &mut self.pending {
                // Same timestamp as the tick in hand: this group is the rest
                // of a tick that straddled a chunk or row-group boundary.
                Some((pending_ts, pending_bars)) if *pending_ts == ts => {
                    pending_bars.extend(bars);
                }
                _ => {
                    if let Some(complete) = self.pending.replace((ts, bars)) {
                        return Ok(Some(complete));
                    }
                }
            }
        }
    }

    /// The next decoded group, pulling chunks from the workers in rotation.
    fn next_group(&mut self) -> Result<Option<Tick>, BacktestError> {
        loop {
            if let Some(group) = self.buffered.next() {
                return Ok(Some(group));
            }
            if self.finished {
                return Ok(None);
            }
            match self.inboxes[self.turn].recv() {
                Ok(Message::Ticks(path, chunk)) => {
                    self.source = Some(path);
                    self.buffered = chunk.into_iter();
                }
                Ok(Message::UnitDone) => self.turn = (self.turn + 1) % self.inboxes.len(),
                // The worker whose turn it is has no units left. Units were
                // dealt in rotation, so no later worker has one either.
                Ok(Message::Finished) => {
                    self.finished = true;
                    return Ok(None);
                }
                Ok(Message::Failed(e)) => {
                    self.finished = true;
                    return Err(*e);
                }
                // Disconnected without a `Finished`: the worker panicked.
                // Treating that as end of data would silently truncate the
                // backtest, so it is an error instead.
                Err(_) => {
                    self.finished = true;
                    return Err(BacktestError::ReaderThreadDied);
                }
            }
        }
    }

    fn out_of_order(&self, at: i64, stream_at: i64) -> BacktestError {
        BacktestError::OutOfOrderData {
            path: self.source.as_deref().cloned().unwrap_or_default(),
            at: ts_to_datetime(at).unwrap_or_default(),
            stream_at: ts_to_datetime(stream_at).unwrap_or_default(),
        }
    }
}

impl Drop for TickStream {
    fn drop(&mut self) {
        // Dropping the receivers makes the workers' next send fail, which is
        // how an early `break` out of the tick loop stops them.
        self.inboxes.clear();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn run_worker(units: Vec<Unit>, subscribed: &SubscriptionMask, tx: &SyncSender<Message>) {
    for unit in units {
        match decode_unit(&unit, subscribed, tx) {
            Ok(Decoded::Complete) => {
                if tx.send(Message::UnitDone).is_err() {
                    return; // consumer went away
                }
            }
            Ok(Decoded::ConsumerGone) => return,
            Err(e) => {
                let _ = tx.send(Message::Failed(Box::new(e)));
                return;
            }
        }
    }
    let _ = tx.send(Message::Finished);
}

enum Decoded {
    Complete,
    ConsumerGone,
}

/// Decode one row group, sending its ticks in batch-sized chunks.
fn decode_unit(
    unit: &Unit,
    subscribed: &SubscriptionMask,
    tx: &SyncSender<Message>,
) -> Result<Decoded, BacktestError> {
    let reader = open_row_group_reader(&unit.path, subscribed, unit.row_group)?;
    // Order is checked within the unit here and across units by the consumer,
    // which together cover every adjacent pair of rows in the stream.
    let mut last_ts: Option<i64> = None;

    for batch in reader {
        let batch = batch.map_err(|e| BacktestError::Parquet {
            path: unit.path.as_ref().clone(),
            message: e.to_string(),
        })?;
        let cols = BarColumns::extract(&batch, &unit.path)?;
        let mut ticks: Vec<Tick> = Vec::new();

        for i in 0..batch.num_rows() {
            let ts = cols.ts.value(i);
            // A timestamp outside the representable range is a corrupt row:
            // dropped before the order check, as in TickReader.
            let Some(time) = ts_to_datetime(ts) else { continue };
            if let Some(last) = last_ts {
                if ts < last {
                    return Err(BacktestError::OutOfOrderData {
                        path: unit.path.as_ref().clone(),
                        at: time,
                        stream_at: ts_to_datetime(last).expect("validated on a previous row"),
                    });
                }
            }
            last_ts = Some(ts);

            let ticker_id = cols.ticker.value(i);
            if !subscribed.contains(ticker_id) {
                continue;
            }
            let Some(bar) = cols.bar_with_time(i, time) else { continue };
            let entry = (Symbol::from_ticker_id(ticker_id), bar);
            match ticks.last_mut() {
                Some((group_ts, bars)) if *group_ts == ts => bars.push(entry),
                _ => ticks.push((ts, vec![entry])),
            }
        }

        if !ticks.is_empty() && tx.send(Message::Ticks(Arc::clone(&unit.path), ticks)).is_err() {
            return Ok(Decoded::ConsumerGone);
        }
    }
    Ok(Decoded::Complete)
}

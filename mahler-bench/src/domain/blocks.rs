//! Blocks world, generalized to an arbitrary number of hands.
//!
//! The planning method is the block-stacking algorithm from
//! N. Gupta and D. S. Nau, "On the complexity of blocks-world planning",
//! Artificial Intelligence 56(2-3):223-254, 1992, as implemented in
//! [GTPyhop](https://github.com/dananau/GTPyhop/blob/main/Examples/blocks_hgn/methods.py):
//!
//! - A block is *done* when it is where the target wants it and every block below it
//!   is done too. Done blocks are never moved.
//! - If there is a clear block that can be moved to its final location, move it there.
//! - Otherwise, move the clear blocks that are in the wrong place to the table, to
//!   unblock the ones below them.
//!
//! The generalization to `h` hands batches up to `h` non-interfering moves per method
//! expansion: all the blocks of a batch are picked up first (each into a different hand),
//! then all of them are put down. With `h = 1` this reduces to the original algorithm.

use std::collections::BTreeSet;

use mahler::extract::{Args, System, Target, View};
use mahler::job::{none, update};
use mahler::state::{List, Map, State};
use mahler::task::{Handler, Task};
use mahler::worker::{Ready, Worker};
use serde::{Deserialize, Serialize};

pub type BlockId = String;
pub type HandId = String;

/// Where a block sits: on another block, on the table, or in one of the hands
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub enum Location {
    Blk(BlockId),
    Table,
    Hand(HandId),
}

impl State for Location {
    type Target = Self;
}

impl Location {
    fn is_block(&self) -> bool {
        matches!(self, Location::Blk(_))
    }

    fn is_hand(&self) -> bool {
        matches!(self, Location::Hand(_))
    }
}

pub type Blocks = Map<BlockId, Location>;

#[derive(Serialize, Deserialize, Debug)]
pub struct World {
    pub blocks: Blocks,
    /// The available hands. Part of the system state but not of the target, so the
    /// planner never tries to converge on it.
    pub hands: List<HandId>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorldTarget {
    pub blocks: Blocks,
}

impl State for World {
    type Target = WorldTarget;
}

/// A location is clear if no block sits on it. The table always is.
fn is_clear(blocks: &Blocks, loc: &Location) -> bool {
    if loc.is_block() || loc.is_hand() {
        return blocks.iter().all(|(_, l)| l != loc);
    }
    true
}

/// The first hand not holding a block, if any
fn free_hand(sys: &World) -> Option<HandId> {
    sys.hands
        .iter()
        .find(|h| is_clear(&sys.blocks, &Location::Hand((*h).clone())))
        .cloned()
}

fn all_clear(blocks: &Blocks) -> Vec<&BlockId> {
    blocks
        .iter()
        .filter(|(b, _)| is_clear(blocks, &Location::Blk((*b).clone())))
        .map(|(b, _)| b)
        .collect()
}

/// Pick a block up from the table into the hand given as target
fn pickup(
    mut loc: View<Location>,
    Target(tgt): Target<Location>,
    System(sys): System<World>,
    Args(block): Args<BlockId>,
) -> Option<View<Location>> {
    // only reachable via `take`, which targets a free hand
    if *loc == Location::Table
        && tgt.is_hand()
        && is_clear(&sys.blocks, &Location::Blk(block))
        && is_clear(&sys.blocks, &tgt)
    {
        *loc = tgt;
        return Some(loc);
    }

    None
}

/// Pick a block up from another block into the hand given as target
fn unstack(
    mut loc: View<Location>,
    Target(tgt): Target<Location>,
    System(sys): System<World>,
    Args(block): Args<BlockId>,
) -> Option<View<Location>> {
    if loc.is_block()
        && tgt.is_hand()
        && is_clear(&sys.blocks, &Location::Blk(block))
        && is_clear(&sys.blocks, &tgt)
    {
        *loc = tgt;
        return Some(loc);
    }

    None
}

/// Put a held block down on the table
fn putdown(mut loc: View<Location>) -> Option<View<Location>> {
    if loc.is_hand() {
        *loc = Location::Table;
        return Some(loc);
    }

    None
}

/// Put a held block down on top of the block given as target
fn stack(
    mut loc: View<Location>,
    Target(tgt): Target<Location>,
    System(sys): System<World>,
) -> Option<View<Location>> {
    if loc.is_hand() && tgt.is_block() && is_clear(&sys.blocks, &tgt) {
        *loc = tgt;
        return Some(loc);
    }

    None
}

/// Grab a block with a free hand, either from the table or from another block
fn take(
    loc: View<Location>,
    System(sys): System<World>,
    Args(block): Args<BlockId>,
) -> Option<Task> {
    if !is_clear(&sys.blocks, &Location::Blk(block)) {
        return None;
    }

    let hand = Location::Hand(free_hand(&sys)?);
    if *loc == Location::Table {
        Some(pickup.with_target(hand))
    } else if loc.is_block() {
        Some(unstack.with_target(hand))
    } else {
        None
    }
}

/// Release a held block, either onto the table or onto another block
fn put(loc: View<Location>, Target(tgt): Target<Location>) -> Option<Task> {
    if loc.is_hand() {
        if tgt == Location::Table {
            return Some(putdown.into_task());
        }
        return Some(stack.with_target(tgt));
    }

    None
}

/// Emit `take`/`put` pairs for the given moves, in batches of `hands` moves
///
/// Within a batch every block is taken first and released afterwards, so a batch of
/// size `n` keeps `n` hands busy at once.
fn emit(moves: Vec<(BlockId, Location)>, hands: usize) -> Vec<Task> {
    let mut tasks = Vec::with_capacity(moves.len() * 2);
    for batch in moves.chunks(hands.max(1)) {
        for (blk, _) in batch {
            tasks.push(take.with_arg("block", blk.clone()));
        }
        for (blk, tgt) in batch {
            tasks.push(put.with_arg("block", blk.clone()).with_target(tgt.clone()));
        }
    }

    tasks
}

/// Two moves interfere if they compete for the same block or the same support
fn interferes(chosen: &[(BlockId, Location)], blk: &BlockId, tgt: &Location) -> bool {
    chosen.iter().any(|(b, t)| {
        // the same block cannot be moved twice
        b == blk
            // two blocks cannot be stacked on the same support (the table fits any number)
            || (t == tgt && tgt != &Location::Table)
            // a block being moved cannot be used as a support
            || t == &Location::Blk(blk.clone())
            || tgt == &Location::Blk(b.clone())
    })
}

/// A block is done when it is where the target wants it and everything below it is
/// done too, which means it never needs to be moved again
fn done_blocks(blocks: &Blocks, target: &Blocks) -> BTreeSet<BlockId> {
    let mut done: BTreeSet<BlockId> = BTreeSet::new();

    // A block can only be done if its support is done, so walking the map until
    // nothing new is marked settles every support chain
    loop {
        let mut found = false;
        for (blk, loc) in blocks.iter() {
            if done.contains(blk) || target.get(blk) != Some(loc) {
                continue;
            }

            let settled = match loc {
                Location::Table => true,
                Location::Blk(support) => done.contains(support),
                Location::Hand(_) => false,
            };

            if settled {
                done.insert(blk.clone());
                found = true;
            }
        }

        if !found {
            return done;
        }
    }
}

/// What a clear block needs from the plan
enum Status {
    /// The block is where it belongs and can be left alone
    Done,
    /// The block can be moved to its target location right now
    Movable(Location),
    /// The block has to move, but its target location is not ready yet
    Waiting,
}

fn status(blocks: &Blocks, done: &BTreeSet<BlockId>, blk: &BlockId, tgt: &Location) -> Status {
    if done.contains(blk) {
        return Status::Done;
    }

    match tgt {
        // the table is always ready to take a block
        Location::Table => Status::Movable(Location::Table),
        // stacking on a block that is not done means moving the block again later
        Location::Blk(support) if done.contains(support) && is_clear(blocks, tgt) => {
            Status::Movable(tgt.clone())
        }
        _ => Status::Waiting,
    }
}

fn move_blks(
    blocks: View<Blocks>,
    Target(target): Target<Blocks>,
    System(sys): System<World>,
) -> Vec<Task> {
    let hands = sys.hands.len().max(1);
    let done = done_blocks(&blocks, &target);

    let mut movable: Vec<(BlockId, Location)> = Vec::new();
    let mut waiting: Vec<(BlockId, Location)> = Vec::new();

    for blk in all_clear(&blocks) {
        // the target is assumed to be well formed
        let tgt_loc = target.get(blk).unwrap();

        match status(&blocks, &done, blk, tgt_loc) {
            Status::Done => {}
            Status::Movable(to) => {
                if !interferes(&movable, blk, &to) && movable.len() < hands {
                    movable.push((blk.clone(), to));
                }
            }
            Status::Waiting => {
                // a block already on the table is out of the way already
                if blocks.get(blk) != Some(&Location::Table) {
                    waiting.push((blk.clone(), Location::Table));
                }
            }
        }
    }

    // prefer moves that put a block where it will not have to move again
    if !movable.is_empty() {
        return emit(movable, hands);
    }

    // otherwise clear the way by moving blocks that are in the wrong place to the table
    emit(waiting, hands)
}

/// Build a worker for the blocks world
pub fn worker(initial: World) -> Worker<World, Ready> {
    Worker::new()
        .jobs(
            "/blocks/{block}",
            [
                none(pickup)
                    .with_description(|Args(block): Args<String>| format!("pick up {block}")),
                none(unstack)
                    .with_description(|Args(block): Args<String>| format!("unstack {block}")),
                none(putdown)
                    .with_description(|Args(block): Args<String>| format!("put down {block}")),
                none(stack).with_description(
                    |Args(block): Args<String>, Target(tgt): Target<Location>| match tgt {
                        Location::Blk(on) => format!("stack {block} on {on}"),
                        other => format!("stack {block} on {other:?}"),
                    },
                ),
                none(take),
                none(put),
            ],
        )
        .job("/blocks", update(move_blks))
        .initial_state(initial)
        .expect("blocks world initial state should be valid")
}

pub fn block_id(index: usize) -> BlockId {
    format!("b{index:03}")
}

fn hand_ids(hands: usize) -> List<HandId> {
    (0..hands).map(|i| format!("h{i}")).collect()
}

/// A single tower `b0 .. bN` (b0 on the table) that has to be turned upside down
///
/// Every block can be moved straight to its final location, so the plan is exactly
/// `n` moves long and the `move to table` branch of the method is never used.
pub fn tower_reversal(blocks: usize, hands: usize) -> (World, WorldTarget) {
    let initial = (0..blocks)
        .map(|i| {
            let loc = if i == 0 {
                Location::Table
            } else {
                Location::Blk(block_id(i - 1))
            };
            (block_id(i), loc)
        })
        .collect::<Blocks>();

    let target = (0..blocks)
        .map(|i| {
            let loc = if i == blocks - 1 {
                Location::Table
            } else {
                Location::Blk(block_id(i + 1))
            };
            (block_id(i), loc)
        })
        .collect::<Blocks>();

    (
        World {
            blocks: initial,
            hands: hand_ids(hands),
        },
        WorldTarget { blocks: target },
    )
}

/// A single tower that has to be rebuilt with the bottom block moved to the top
///
/// No block can go straight to its final location, so the whole tower is taken apart
/// onto the table before being rebuilt: roughly `2n` moves, exercising both branches
/// of the method.
pub fn tower_rebuild(blocks: usize, hands: usize) -> (World, WorldTarget) {
    let initial = (0..blocks)
        .map(|i| {
            let loc = if i == 0 {
                Location::Table
            } else {
                Location::Blk(block_id(i - 1))
            };
            (block_id(i), loc)
        })
        .collect::<Blocks>();

    // b1 on the table, b2 on b1, ... bN on b(N-1), and b0 on top of the tower
    let target = (0..blocks)
        .map(|i| {
            let loc = if i == 0 {
                Location::Blk(block_id(blocks - 1))
            } else if i == 1 {
                Location::Table
            } else {
                Location::Blk(block_id(i - 1))
            };
            (block_id(i), loc)
        })
        .collect::<Blocks>();

    (
        World {
            blocks: initial,
            hands: hand_ids(hands),
        },
        WorldTarget { blocks: target },
    )
}

/// Several independent towers, each one turned upside down
///
/// Every tower can progress on its own, so at each step there are `towers` blocks that
/// can be moved to their final location and a worker with `h` hands moves `h` of them
/// per method expansion. This is the scenario where the number of hands changes the
/// shape of the plan.
pub fn independent_towers(towers: usize, height: usize, hands: usize) -> (World, WorldTarget) {
    let name = |tower: usize, index: usize| format!("t{tower}b{index:03}");

    let mut initial = Blocks::new();
    let mut target = Blocks::new();
    for tower in 0..towers {
        for index in 0..height {
            let from = if index == 0 {
                Location::Table
            } else {
                Location::Blk(name(tower, index - 1))
            };
            let to = if index == height - 1 {
                Location::Table
            } else {
                Location::Blk(name(tower, index + 1))
            };

            initial.insert(name(tower, index), from);
            target.insert(name(tower, index), to);
        }
    }

    (
        World {
            blocks: initial,
            hands: hand_ids(hands),
        },
        WorldTarget { blocks: target },
    )
}

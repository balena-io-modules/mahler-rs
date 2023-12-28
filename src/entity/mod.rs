mod read;
mod target;
mod update;

pub use read::*;
pub use target::*;
pub use update::*;

use crate::system::Indexable;

pub trait WithParent {
    type Parent: Indexable;

    fn parent_id(&self) -> <Self::Parent as Indexable>::Id;
}

pub trait Entity: Indexable + WithParent {}

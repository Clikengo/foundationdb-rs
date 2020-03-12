use crate::tuple::Subspace;
use crate::{Directory, DirectoryError};
use std::result;

pub type DirectorySubspaceResult = result::Result<DirectorySubspace, DirectoryError>;

pub struct DirectorySubspace {
    subspace: Subspace,
    dl: Directory,
    path: Vec<String>,
    layer: Vec<u8>
}

// TODO: impl { .. }
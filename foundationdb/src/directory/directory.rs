use crate::Transaction;
use crate::tuple::{Subspace, pack_into, pack};
use crate::tuple::hca::HighContentionAllocator;

use super::*;
use std::result;
use crate::DirectoryError::Version;
use crate::directory::directory_subspace::{DirectorySubspaceResult, DirectorySubspace};

const LAYER_VERSION: (u8, u8, u8) = (1, 0, 0);
const MAJOR_VERSION: u32 = 1;
const MINOR_VERSION: u32 = 0;
const PATCH_VERSION: u32 = 0;
const DEFAULT_NODE_PREFIX:&[u8] =  b"\xFE";

const SUBDIRS:u8 = 0;

#[derive(PartialEq)]
enum PermissionLevel {
    Read,
    Write
}

pub type DirectoryResult = result::Result<Directory, DirectoryError>;

pub struct Directory {
    node_prefix: Subspace,
    content_prefix: Subspace,

    allow_manual_prefixes: bool,

    allocator: HighContentionAllocator,
    root_node: Subspace,

    path: Vec<String>,
    layer: Vec<u8>,
}

impl Directory {

    pub fn root() -> Directory {
        Directory {
            node_prefix: DEFAULT_NODE_PREFIX.into(),
            content_prefix: Subspace::from_bytes(b""),

            allow_manual_prefixes: false,

            allocator: HighContentionAllocator::new(Subspace::from_bytes(b"hca")),
            root_node: DEFAULT_NODE_PREFIX.into(),

            path: Vec::new(),
            layer: Vec::new()
        }
    }

    pub fn contents_of_node(&self, node: Subspace, path: &[String], layer: &[u8]) -> DirectorySubspaceResult {


        Ok(DirectorySubspace)
    }

    // pub fn new(parent_node: Directory, path: &[String], layer: &[u8]) -> Directory {
    //     Directory {
    //
    //         allow_manual_prefixes: true,
    //
    //         allocator: HighContentionAllocator::new(Subspace::from_bytes(b"hca")),
    //
    //         root_node: parent_node.node_prefix.clone(),
    //         path: path.to_vec(),
    //         layer: layer.to_vec(),
    //
    //     }
    //
    // }

    pub async fn create_or_open(&self, trx: Transaction, path: &[&str], layer: &[u8], prefix: &[u8], allow_create: bool, allow_open: bool) -> DirectoryResult {
        self.check_version(&trx, PermissionLevel::Read).await?;

        if prefix.len() > 0 && !self.allow_manual_prefixes {
            if self.path.len() == 0 {
                return Err(DirectoryError::Message("cannot specify a prefix unless manual prefixes are enabled".to_string()))
            }

            return Err(DirectoryError::Message("cannot specify a prefix in a partition".to_string()))
        }

        if path.len() == 0 {
            return Err(DirectoryError::CannotOpenRoot)
        }

        // FIND

        if !allow_create {
            return Err(DirectoryError::NotExist)
        }

        // self.initialize_directory(&trx);

        if prefix.len() == 0 {
            // let new_subspace = self.allocator.allocate(&trx).await?;
            // TODO: maybe check range and prefix free but I think the allocate does that already
        } else {
            let is_prefix_free = self.is_prefix_free(&trx, prefix).await?;
        }
        //
        // if layer != self.get_layer() && layer != &[] {
        //     return Err(DirectoryError::LayerMismatch);
        // }

        Ok(Directory::root())
    }

    // pub async fn find(&self, trx: Transaction, path: &[&str]) -> DirectoryResult {
    //
    // }

    // pub async fn initialize_directory(&self, trx: &Transaction) {
    //     let version = [MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION].to_le_bytes();
    //     let version_subspace: &[u8] =  b"version";
    //     let version_key = self.root_node.subspace(&version_subspace);
    //
    //     trx.set(version_key.bytes(), version).await;
    // }

    async fn is_prefix_free(&self, trx: &Transaction, prefix: &[u8]) -> Result<bool, DirectoryError> {

        if prefix.len() == 0 {
            return Ok(false);
        }

        Ok(true)
    }


    async fn check_version(&self, trx: &Transaction, perm_level: PermissionLevel ) -> Result<(), DirectoryError> {
        let version_subspace: &[u8] =  b"version";
        let version_key = self.root_node.subspace(&version_subspace);
        let version_opt = trx.get(version_key.bytes(), false).await?;

        match version_opt {
            None => {
                if perm_level == PermissionLevel::Write {
                    //init
                    return Err(Version("fix soon".to_string()));
                }

                Ok(())
            }
            Some(versions) => {
                if versions.len() < 12 {
                    return Err(Version("incorrect version length".to_string()));
                }
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&versions[0..4]);
                let major: u32 = u32::from_be_bytes(arr);

                arr.copy_from_slice(&versions[4..8]);
                let minor: u32 = u32::from_be_bytes(arr);

                arr.copy_from_slice(&versions[8..12]);
                let patch: u32 = u32::from_be_bytes(arr);

                if major > MAJOR_VERSION {
                    let msg = format!("cannot load directory with version {}.{}.{} using directory layer {}.{}.{}", major, minor, patch, MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);
                    return Err(Version(msg))
                }

                if minor > MINOR_VERSION && perm_level == PermissionLevel::Write {
                    let msg = format!("directory with version {}.{}.{} is read-only when opened using directory layer {}.{}.{}", major, minor, patch, MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);
                    return Err(Version(msg))
                }

                Ok(())
            }
        }
    }

    // pub async fn find(&self, trx: &Transaction, path: &[&str]) -> DirectoryResult {
    //     let mut node = Directory::root();
    //
    //     for path_name in path {
    //         let mut node_layer_id = vec!(SUBDIRS);
    //         pack_into(&path_name, &mut node_layer_id);
    //         let new_node = node.node_prefix.subspace(&node_layer_id);
    //
    //         match trx.get(new_node.bytes(), false).await {
    //             Err(_) => {
    //                 return Ok(node);
    //             }
    //             Result(node_name) => {
    //                 let ss = node.node_with_prefix(key);
    //                 node.node_prefix = ss;
    //                 node.path.push(path_name.to_string())
    //             }
    //         }
    //     }
    //
    //
    //     Ok(node)
    // }



    pub fn get_layer(&self) -> &[u8] {
        self.layer.as_slice()
    }
}


use crate::tuple::{Subspace};
use crate::future::FdbSlice;
use crate::{Transaction, Directory, DirectoryResult};
use crate::directory::directory_subspace::DirectorySubspaceResult;


pub struct Node {
    subspace: Subspace,
    path: Vec<String>,
    target_path: Vec<String>,
    _layer: Option<Vec<u8>>
}

impl Node {

    pub fn exists(&self) -> bool {
        // if self.subspace == None {
        //     return false;
        // }

        true
    }

    pub async fn prefetchMetadata(&self, trx: &Transaction) -> &Node {
        if self.exists() {
           self.layer(trx).await;
        }

        return self;
    }

    pub async fn layer(&mut self, trx: &Transaction) -> &[u8] {
        if self._layer == None {
            let key = self.subspace.subspace(&b"layer".to_vec());
            self._layer = match trx.get(key.bytes(), false).await {
                Ok(None) => Some(Vec::new()),
                Err(_) => Some(Vec::new()),
                Ok(Some(fv)) => Some(fv.to_vec())
            }
        }

        return self._layer.unwrap().as_slice()
    }

    pub async fn is_in_partition(&mut self, trx: Transaction, include_empty_subpath: bool) -> bool {
        if !self.exists() {
            return false
        }

        self.layer(&trx).await == b"partition" &&
            (include_empty_subpath || self.target_path.len() > self.path.len())
    }

    pub fn get_partition_subpath(&self) -> &[String] {
        self.target_path[..self.path.len()].clone()
    }

    pub async fn get_contents(self, directory: Directory, trx: &Transaction ) -> DirectorySubspaceResult {
        directory.contents_of_node(self.subspace, &self.path, &self.layer(trx).await)
    }
}





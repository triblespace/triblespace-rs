use std::marker::PhantomData;
use std::rc::Rc;

use triblespace::core::patch::{IdentitySchema, KeySchema, KeySegmentation, PATCH};

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
struct TypeOnlySegmentation(PhantomData<Rc<()>>);

impl KeySegmentation<1> for TypeOnlySegmentation {
    const SEGMENTS: [usize; 1] = [0];
}

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
struct TypeOnlySchema(PhantomData<Rc<()>>);

impl KeySchema<1> for TypeOnlySchema {
    type Segmentation = TypeOnlySegmentation;
    const SEGMENT_PERM: &'static [usize] = &[0];
    const KEY_TO_TREE: [usize; 1] = [0];
    const TREE_TO_KEY: [usize; 1] = [0];
}

fn assert_send_sync<T: Send + Sync>() {}

fn main() {
    assert_send_sync::<PATCH<1, IdentitySchema, ()>>();
    assert_send_sync::<PATCH<1, TypeOnlySchema, ()>>();
}

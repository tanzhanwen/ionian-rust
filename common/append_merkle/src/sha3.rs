use crate::{Algorithm, HashElement};
use tiny_keccak::{Hasher, Keccak};

/// MT leaf hash prefix
pub const LEAF: u8 = 0x00;
/// MT interior node hash prefix
const INTERIOR: u8 = 0x01;

pub struct Sha3Algorithm {}

impl<E: HashElement> Algorithm<E> for Sha3Algorithm {
    fn parent(left: &E, right: &E) -> E {
        let mut h = Keccak::v256();
        let mut e = E::end_pad();
        h.update(&[INTERIOR]);
        h.update(left.as_ref());
        h.update(right.as_ref());
        h.finalize(e.as_mut());
        e
    }

    fn leaf(data: &[u8]) -> E {
        let mut h = Keccak::v256();
        let mut e = E::end_pad();
        h.update(&[LEAF]);
        h.update(data.as_ref());
        h.finalize(e.as_mut());
        e
    }
}

//! Transfers the actual Native pool allocation, not a reservation for an unrelated Rust Vec.
use std::ffi::c_void;
use std::ptr::NonNull;

/// The owner must remain valid until release is called, possibly on a Tokio thread.
/// release must be noexcept, free the exact allocation, and destroy the retained pool owner.
#[repr(C)]
pub struct NativePayload {
    pub data: *mut u8,
    pub len: usize,
    pub owner: *mut c_void,
    pub release: Option<unsafe extern "C" fn(*mut c_void)>,
}

pub(crate) struct OwnedPayload {
    data: NonNull<u8>,
    len: usize,
    owner: NonNull<c_void>,
    release: unsafe extern "C" fn(*mut c_void),
}

// The C++ allocation/pool owner is explicitly cross-thread; no other code may mutate it after
// transfer. Bytes pins this owner and calls AsRef once before exposing immutable clones.
unsafe impl Send for OwnedPayload {}

impl OwnedPayload {
    /// Ownership transfers on entry when the release/owner pair is valid, including validation
    /// errors. Malformed owner/release pairs cannot be safely recovered and remain the caller's.
    pub(crate) unsafe fn take(value: NativePayload) -> Result<Self, String> {
        let release = value.release.ok_or("Physical payload requires a Native allocation release callback")?;
        let owner = NonNull::new(value.owner).ok_or("Physical payload requires its Native pool owner")?;
        let data = match NonNull::new(value.data) {
            Some(data) if value.len > 0 && value.len <= isize::MAX as usize => data,
            _ => {
                release(owner.as_ptr());
                return Err("Physical payload requires a nonempty bounded Native allocation".into());
            }
        };
        Ok(Self { data, len: value.len, owner, release })
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        // Exclusively held before transfer to Bytes; C++ relinquishes all access on entry.
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.len) }
    }

    pub(crate) fn into_bytes(self) -> bytes::Bytes { bytes::Bytes::from_owner(self) }
}

impl AsRef<[u8]> for OwnedPayload {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len) }
    }
}

impl Drop for OwnedPayload {
    fn drop(&mut self) { unsafe { (self.release)(self.owner.as_ptr()); } }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // A real allocated buffer owner at the C contract boundary, not an allocator
    // reservation. Production owners are actual C++ task-pool allocations.
    struct BufferOwner { bytes: Vec<u8>, releases: Arc<AtomicUsize> }

    unsafe extern "C" fn release(owner: *mut c_void) {
        let owner = Box::from_raw(owner.cast::<BufferOwner>());
        owner.releases.fetch_add(1, Ordering::AcqRel);
        drop(owner);
    }

    fn payload() -> (NativePayload, Arc<AtomicUsize>) {
        let releases = Arc::new(AtomicUsize::new(0));
        let mut owner = Box::new(BufferOwner { bytes: vec![1, 2, 3, 4], releases: releases.clone() });
        let data = owner.bytes.as_mut_ptr();
        let len = owner.bytes.len();
        (NativePayload { data, len, owner: Box::into_raw(owner).cast(), release: Some(release) }, releases)
    }

    #[test]
    fn actual_buffer_is_shared_until_the_last_cross_thread_body_clone_is_dropped() {
        let (payload, releases) = payload();
        let pointer = payload.data;
        let mut owned = unsafe { OwnedPayload::take(payload) }.unwrap();
        owned.as_mut_slice()[1] = 9;
        let bytes = owned.into_bytes();
        assert_eq!(bytes.as_ptr(), pointer.cast_const());
        assert_eq!(bytes.as_ref(), &[1, 9, 3, 4]);
        let clone = bytes.clone();
        drop(bytes);
        assert_eq!(releases.load(Ordering::Acquire), 0);
        std::thread::spawn(move || {
            assert_eq!(clone.as_ref(), &[1, 9, 3, 4]);
            drop(clone);
        }).join().unwrap();
        assert_eq!(releases.load(Ordering::Acquire), 1);
    }

    #[test]
    fn invalid_data_or_length_with_a_valid_owner_releases_exactly_once() {
        for invalid_length in [0, usize::MAX] {
            let (mut payload, releases) = payload();
            payload.len = invalid_length;
            assert!(unsafe { OwnedPayload::take(payload) }.is_err());
            assert_eq!(releases.load(Ordering::Acquire), 1);
        }
        let (mut payload, releases) = payload();
        payload.data = std::ptr::null_mut();
        assert!(unsafe { OwnedPayload::take(payload) }.is_err());
        assert_eq!(releases.load(Ordering::Acquire), 1);
    }

    #[test]
    fn unpublished_owned_payload_releases_its_actual_buffer() {
        let (payload, releases) = payload();
        let owned = unsafe { OwnedPayload::take(payload) }.unwrap();
        assert_eq!(releases.load(Ordering::Acquire), 0);
        drop(owned);
        assert_eq!(releases.load(Ordering::Acquire), 1);
    }
}

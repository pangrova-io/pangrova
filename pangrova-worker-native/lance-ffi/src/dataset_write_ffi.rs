use std::sync::atomic::Ordering;

use crate::write_drain::LanceCWriteDrainEvidence;
use crate::{join_writer, set_last_error, LanceCWriter};

/// Cooperative cancellation retains the writer until its actual SDK work joins.
#[no_mangle]
pub extern "C" fn lance_ffi_writer_drain(
    writer: *mut LanceCWriter,
    evidence: *mut LanceCWriteDrainEvidence,
) -> i32 {
    ffi_guard!("writer_drain", -1, {
        if writer.is_null() || evidence.is_null() {
            set_last_error("Dataset drain requires its live handle and evidence output");
            return -1;
        }
        unsafe {
            *evidence = LanceCWriteDrainEvidence::default();
        }
        let writer = unsafe { &mut *writer };
        if !writer.finished {
            writer.cancelled.store(true, Ordering::Release);
        }
        let result = join_writer(writer);
        unsafe {
            *evidence = LanceCWriteDrainEvidence {
                encoders_and_uploads_joined: u32::from(writer.drained),
                // Dataset writes have no private local FileStage directory.
                // Their SDK objects remain fenced by the durable journal.
                private_stage_cleaned: u32::from(writer.drained),
                staging_publication: 0,
            };
        }
        match result {
            Ok(_) => 0,
            Err(error) => {
                set_last_error(&error);
                -1
            }
        }
    })
}

/// Releases joined local owners only; never deletes Dataset objects or sidecars.
#[no_mangle]
pub extern "C" fn lance_ffi_writer_destroy(writer: *mut LanceCWriter) -> i32 {
    ffi_guard!("writer_destroy", -1, {
        if writer.is_null() {
            set_last_error("Dataset destroy requires its live handle");
            return -1;
        }
        if !unsafe { &*writer }.drained {
            set_last_error("Dataset destroy requires actual SDK and writer-thread drain");
            return -1;
        }
        drop(unsafe { Box::from_raw(writer) });
        0
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Float64Array, StructArray};
    use arrow::buffer::{Buffer, ScalarBuffer};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct Values {
        data: Vec<f64>,
        releases: Arc<AtomicUsize>,
    }
    impl Drop for Values {
        fn drop(&mut self) {
            self.releases.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[test]
    fn rejected_dataset_append_consumes_and_clears_actual_arrow_owner_once() {
        let releases = Arc::new(AtomicUsize::new(0));
        let owner = Arc::new(Values {
            data: vec![1.0000000001],
            releases: releases.clone(),
        });
        let pointer = std::ptr::NonNull::new(owner.data.as_ptr().cast_mut().cast()).unwrap();
        let buffer = unsafe { Buffer::from_custom_allocation(pointer, 8, owner) };
        let values = Arc::new(Float64Array::new(ScalarBuffer::new(buffer, 0, 1), None));
        let row = StructArray::new(
            vec![Arc::new(arrow_schema::Field::new(
                "value",
                arrow_schema::DataType::Float64,
                false,
            ))]
            .into(),
            vec![values],
            None,
        );
        let (mut array, mut schema) = arrow::ffi::to_ffi(&row.to_data()).unwrap();
        drop(row);
        assert_eq!(releases.load(Ordering::Acquire), 0);
        assert_eq!(
            crate::lance_ffi_writer_write_batch(std::ptr::null_mut(), &mut array, &mut schema),
            -1
        );
        assert!(array.is_released());
        assert_eq!(releases.load(Ordering::Acquire), 1);
        drop(array);
        drop(schema);
        assert_eq!(releases.load(Ordering::Acquire), 1);
    }

    #[test]
    fn dataset_drain_and_destroy_refuse_missing_owner_without_fabricating_evidence() {
        let mut evidence = crate::write_drain::LanceCWriteDrainEvidence::default();
        assert_eq!(
            super::lance_ffi_writer_drain(std::ptr::null_mut(), &mut evidence),
            -1
        );
        assert_eq!(evidence.encoders_and_uploads_joined, 0);
        assert_eq!(evidence.private_stage_cleaned, 0);
        assert_eq!(super::lance_ffi_writer_destroy(std::ptr::null_mut()), -1);
        assert_eq!(crate::lance_ffi_abi_version(), 13);
    }
}

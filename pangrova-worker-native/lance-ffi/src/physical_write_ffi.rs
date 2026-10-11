//! Physical-file C boundary. All execution uses the canonical binding and real SDK FileWriter.
use std::ffi::c_char;
use std::path::Path;
use std::ptr;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StructArray};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use lance_io::object_writer::ObjectWritePublication;
use tokio::runtime::Runtime;

use crate::physical_payload::{NativePayload, OwnedPayload};
use crate::physical_write::{PhysicalWriter, StageConfig};
use crate::write_drain::LanceCWriteDrainEvidence;
use crate::{
    get_or_create_runtime, into_c_buffer, parse_required_string, parse_task_attempt,
    set_last_error, LanceCBuffer, LanceCStorage,
};

#[repr(C)]
pub struct LanceCPhysicalWriterOptions {
    pub binding: *const u8,
    pub binding_len: usize,
    pub task_attempt: *const u8,
    pub task_attempt_len: usize,
    pub tenant_id: *const c_char,
    pub spill_directory: *const c_char,
    pub spill_bytes: u64,
    pub object_write_bytes: u64,
}

pub struct LanceCPhysicalWriter {
    runtime: Arc<Runtime>,
    inner: PhysicalWriter,
    failed: bool,
}

#[no_mangle]
pub extern "C" fn lance_ffi_physical_writer_open(
    storage: *const LanceCStorage,
    options: *const LanceCPhysicalWriterOptions,
) -> *mut LanceCPhysicalWriter {
    ffi_guard!("physical_writer_open", ptr::null_mut(), {
        match open(storage, options) {
            Ok(writer) => Box::into_raw(Box::new(writer)),
            Err(error) => {
                set_last_error(&error);
                ptr::null_mut()
            }
        }
    })
}

fn open(
    storage: *const LanceCStorage,
    options: *const LanceCPhysicalWriterOptions,
) -> Result<LanceCPhysicalWriter, String> {
    if storage.is_null() || options.is_null() {
        return Err("Physical writer requires storage and options".into());
    }
    let options = unsafe { &*options };
    if options.binding.is_null()
        || options.binding_len == 0
        || options.binding_len > 64 * 1024 * 1024
    {
        return Err("Physical writer binding violates its pointer/size contract".into());
    }
    let tenant = parse_required_string(options.tenant_id, "tenant_id", 1024)?;
    let directory = parse_required_string(options.spill_directory, "spill_directory", 8192)?;
    let attempt = parse_task_attempt(options.task_attempt, options.task_attempt_len)?;
    let binding = unsafe { std::slice::from_raw_parts(options.binding, options.binding_len) };
    let runtime = get_or_create_runtime();
    let inner = runtime.block_on(PhysicalWriter::open(
        unsafe { &*storage }.context.clone(),
        binding,
        attempt,
        &tenant,
        StageConfig {
            directory: Path::new(&directory),
            spill_bytes: options.spill_bytes,
            object_write_bytes: options.object_write_bytes,
        },
    ))?;
    Ok(LanceCPhysicalWriter {
        runtime,
        inner,
        failed: false,
    })
}

#[no_mangle]
pub extern "C" fn lance_ffi_physical_writer_write_batch(
    writer: *mut LanceCPhysicalWriter,
    array: *mut FFI_ArrowArray,
    schema: *const FFI_ArrowSchema,
) -> i32 {
    ffi_guard!("physical_writer_write_batch", -1, {
        // Takes the actual Arrow owner before validating the handle/schema, so every error releases
        // it exactly once. The caller-visible empty slot prevents a second release by C++ RAII.
        if array.is_null() {
            if !writer.is_null() {
                unsafe {
                    (*writer).failed = true;
                }
            }
            set_last_error("Physical write requires an owned Arrow array");
            return -1;
        }
        let owned = unsafe { ptr::read(array) };
        unsafe {
            ptr::write(array, FFI_ArrowArray::empty());
        }
        if writer.is_null() || schema.is_null() {
            if !writer.is_null() {
                unsafe {
                    (*writer).failed = true;
                }
            }
            set_last_error("Physical write requires a live handle and schema");
            return -1;
        }
        let writer = unsafe { &mut *writer };
        if writer.failed {
            set_last_error("Physical writer is poisoned by an earlier failure");
            return -1;
        }
        writer.failed = true;
        let result = unsafe { from_ffi(owned, &*schema) }
            .map_err(|error| error.to_string())
            .and_then(|data| {
                if !matches!(data.data_type(), arrow_schema::DataType::Struct(_)) {
                    return Err("Physical write input must be a complete Arrow struct".into());
                }
                let row = StructArray::from(data);
                if row.null_count() != 0 || row.len() as u64 > crate::MAX_BATCH_ROWS {
                    return Err(
                        "Physical write requires complete non-null rows within the ABI batch bound"
                            .into(),
                    );
                }
                Ok(RecordBatch::from(row))
            });
        let result = result.and_then(|batch| writer.runtime.block_on(writer.inner.write(batch)));
        match result {
            Ok(()) => {
                writer.failed = false;
                0
            }
            Err(error) => {
                set_last_error(&error);
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_ffi_physical_writer_finish_stage(
    writer: *mut LanceCPhysicalWriter,
    bytes: *mut u64,
) -> i32 {
    ffi_guard!("physical_writer_finish_stage", -1, {
        if writer.is_null() || bytes.is_null() {
            set_last_error("Physical close requires handle and byte output");
            return -1;
        }
        let writer = unsafe { &mut *writer };
        if writer.failed {
            set_last_error("Failed physical writer cannot close successfully");
            return -1;
        }
        writer.failed = true;
        match writer.runtime.block_on(writer.inner.finish_stage()) {
            Ok(size) => {
                unsafe {
                    *bytes = size;
                }
                writer.failed = false;
                0
            }
            Err(error) => {
                set_last_error(&error);
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_ffi_physical_writer_finish_empty(
    writer: *mut LanceCPhysicalWriter,
    output: *mut LanceCBuffer,
) -> i32 {
    ffi_guard!("physical_writer_finish_empty", -1, {
        if writer.is_null() || output.is_null() {
            set_last_error("Empty physical close requires handle and receipt output");
            return -1;
        }
        let writer = unsafe { &mut *writer };
        if writer.failed {
            set_last_error("Failed physical writer cannot emit an empty receipt");
            return -1;
        }
        match writer.inner.empty_fragment() {
            Ok(bytes) => {
                unsafe {
                    *output = into_c_buffer(bytes);
                }
                0
            }
            Err(error) => {
                writer.failed = true;
                set_last_error(&error);
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_ffi_physical_writer_publish(
    writer: *mut LanceCPhysicalWriter,
    payload: NativePayload,
    output: *mut LanceCBuffer,
) -> i32 {
    ffi_guard!("physical_writer_publish", -1, {
        let payload = match unsafe { OwnedPayload::take(payload) } {
            Ok(payload) => payload,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        if writer.is_null() || output.is_null() {
            set_last_error("Physical publication requires handle and receipt output");
            return -1;
        }
        let writer = unsafe { &mut *writer };
        if writer.failed {
            set_last_error("Failed physical writer cannot publish");
            return -1;
        }
        writer.failed = true;
        match writer.runtime.block_on(writer.inner.publish(payload)) {
            Ok(bytes) => {
                unsafe {
                    *output = into_c_buffer(bytes);
                }
                0
            }
            Err(error) => {
                set_last_error(&error);
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_ffi_physical_writer_resource_usage(
    writer: *const LanceCPhysicalWriter,
    output: *mut LanceCBuffer,
) -> i32 {
    ffi_guard!("physical_writer_resource_usage", -1, {
        if writer.is_null() || output.is_null() {
            set_last_error("Physical resource usage requires live handle and byte output");
            return -1;
        }
        // A failed append/publish is still observable; this is not a success receipt.
        let writer = unsafe { &*writer };
        let bytes = prost::Message::encode_to_vec(writer.inner.resource_usage());
        unsafe {
            *output = into_c_buffer(bytes);
        }
        0
    })
}

#[no_mangle]
pub extern "C" fn lance_ffi_physical_writer_drain(
    writer: *mut LanceCPhysicalWriter,
    evidence: *mut LanceCWriteDrainEvidence,
) -> i32 {
    ffi_guard!("physical_writer_drain", -1, {
        if writer.is_null() || evidence.is_null() {
            set_last_error("Physical drain requires a live handle and evidence output");
            return -1;
        }
        unsafe {
            *evidence = LanceCWriteDrainEvidence::default();
        }
        let writer = unsafe { &mut *writer };
        let runtime = writer.runtime.clone();
        let result = runtime.block_on(writer.inner.drain());
        unsafe {
            *evidence = LanceCWriteDrainEvidence {
                encoders_and_uploads_joined: u32::from(writer.inner.drained),
                private_stage_cleaned: u32::from(writer.inner.stage_cleaned),
                staging_publication: match writer.inner.staging_publication {
                    None => 0,
                    Some(ObjectWritePublication::NotPublished) => 1,
                    Some(ObjectWritePublication::Published) => 2,
                    Some(ObjectWritePublication::Unknown) => 3,
                },
            };
        }
        match result {
            Ok(()) => 0,
            Err(error) => {
                set_last_error(&error);
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_ffi_physical_writer_destroy(writer: *mut LanceCPhysicalWriter) -> i32 {
    ffi_guard!("physical_writer_destroy", -1, {
        if writer.is_null() {
            set_last_error("Physical destroy requires a live handle");
            return -1;
        }
        if !unsafe { &*writer }.inner.can_destroy() {
            set_last_error(
                "Physical destroy requires joined SDK owners and cleaned private staging",
            );
            return -1;
        }
        // Drops only after the actual retained runtime finished every SDK job.
        // This never deletes the allocated final object, including UNKNOWN PUT.
        drop(unsafe { Box::from_raw(writer) });
        0
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn physical_drain_and_destroy_require_real_owner_and_never_invent_evidence() {
        let mut evidence = super::LanceCWriteDrainEvidence::default();
        assert_eq!(
            super::lance_ffi_physical_writer_drain(std::ptr::null_mut(), &mut evidence),
            -1
        );
        assert_eq!(evidence.encoders_and_uploads_joined, 0);
        assert_eq!(evidence.private_stage_cleaned, 0);
        assert_eq!(evidence.staging_publication, 0);
        assert_eq!(
            super::lance_ffi_physical_writer_destroy(std::ptr::null_mut()),
            -1
        );
        assert_eq!(crate::lance_ffi_abi_version(), 13);
    }

    #[test]
    fn physical_resource_usage_refuses_invalid_handles_without_inventing_observations() {
        let mut output = crate::LanceCBuffer {
            data: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        assert_eq!(
            super::lance_ffi_physical_writer_resource_usage(std::ptr::null(), &mut output),
            -1
        );
        assert!(output.data.is_null());
        assert_eq!(output.len, 0);
        assert_eq!(output.capacity, 0);
        let error = crate::lance_ffi_last_error();
        assert!(!error.is_null());
        let error = unsafe { std::ffi::CStr::from_ptr(error) }.to_str().unwrap();
        assert!(error.contains("requires live handle"));
    }

    #[test]
    fn physical_ffi_guard_is_hygienic_and_preserves_panic_diagnostic() {
        let result = ffi_guard!("physical_guard_test", -1, {
            panic!("actual child-module panic");
        });
        assert_eq!(result, -1);
        let diagnostic = unsafe { std::ffi::CStr::from_ptr(crate::lance_ffi_last_error()) };
        assert_eq!(
            diagnostic.to_str().unwrap(),
            "Lance C FFI physical_guard_test panicked: actual child-module panic"
        );
    }
}

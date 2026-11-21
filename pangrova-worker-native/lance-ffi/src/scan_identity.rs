//! Exact partition-local physical positions from the SDK row-address metadata.
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use lance_core::{utils::address::RowAddress, ROW_ADDR};
use lance_table::format::Fragment;

pub(crate) const POSITION: &str = "$pangrova$row_position";

pub(crate) fn requested(columns: Option<&[String]>) -> bool {
    columns.is_some_and(|columns| columns.iter().any(|column| column == POSITION))
}

/// Copies only the SDK metadata column; all physical data columns retain their buffers.
/// The scanner is ordered, so strict progression detects duplicate offsets across batches
/// without retaining a per-row set or confusing deletion holes with compacted offsets.
pub(crate) fn materialize(
    batch: &RecordBatch,
    partition: &Fragment,
    columns: &[String],
    last_offset: &mut Option<u32>,
) -> Result<RecordBatch, String> {
    if partition.id >= u64::from(RowAddress::TOMBSTONE_FRAG) {
        return Err("Dataset partition ID cannot be represented by an SDK row address".into());
    }
    let physical_rows = partition
        .physical_rows
        .ok_or("Dataset partition has no physical row bound")? as u64;
    if physical_rows > RowAddress::FRAGMENT_SIZE || batch.schema().index_of(POSITION).is_ok() {
        return Err("Dataset physical row bound or reserved position schema is invalid".into());
    }
    let addresses = batch
        .column_by_name(ROW_ADDR)
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or("Dataset scanner did not return the SDK UInt64 row address")?;
    let mut previous = *last_offset;
    let mut positions = Vec::with_capacity(batch.num_rows());
    for row in 0..addresses.len() {
        if addresses.is_null(row) {
            return Err("Dataset scanner returned a null row address".into());
        }
        let address = RowAddress::new_from_u64(addresses.value(row));
        let offset = address.row_offset();
        if u64::from(address.fragment_id()) != partition.id
            || u64::from(offset) >= physical_rows
            || previous.is_some_and(|previous| offset <= previous)
        {
            return Err(
                "Dataset row address differs from its exact partition or physical bounds".into(),
            );
        }
        previous = Some(offset);
        positions.push(i64::from(offset));
    }
    let position = Arc::new(Int64Array::from(positions));
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<arrow_array::ArrayRef> = Vec::with_capacity(columns.len());
    for column in columns {
        if column == POSITION {
            fields.push(Field::new(POSITION, DataType::Int64, false));
            arrays.push(position.clone());
        } else {
            let index = schema.index_of(column).map_err(|error| error.to_string())?;
            fields.push(schema.field(index).clone());
            arrays.push(batch.column(index).clone());
        }
    }
    let result = RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        arrays,
    )
    .map_err(|error| error.to_string())?;
    *last_offset = previous;
    Ok(result)
}

#[cfg(test)]
mod tests;

//! Exact adaptation of task-local variable lists to SDK fixed-size vectors.
//! No numeric coercion, dropped struct fields, or invalid-dimension-to-NULL cast is permitted.
use std::sync::Arc;

use arrow::array::{new_null_array, Array, ArrayRef, FixedSizeListArray, ListArray, MapArray, StructArray};
use arrow::compute::concat;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, FieldRef, SchemaRef};

/// The caller owns the cumulative copy counter. Copies are recorded immediately,
/// including materialized NULL slots, even when a later field fails validation.
/// This measures output-buffer lengths, not allocator traffic or peak memory.
pub(crate) fn adapt_batch(batch: &RecordBatch, schema: SchemaRef, copied_buffer_bytes: &mut u64)
    -> Result<RecordBatch, ArrowError> {
    if schema.fields().is_empty() {
        return Err(invalid("Physical files require at least one allocated column"));
    }
    if batch.num_columns() != schema.fields().len() {
        return Err(invalid("Physical schema column count differs from the task output"));
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, field) in schema.fields().iter().enumerate() {
        if batch.schema().field(index).name() != field.name() {
            return Err(invalid("Physical schema field order/name differs from the task output"));
        }
        let array = adapt_array(batch.column(index), field.data_type(), copied_buffer_bytes)?;
        if !field.is_nullable() && array.null_count() != 0 {
            return Err(invalid("Non-nullable physical column contains NULL"));
        }
        columns.push(array);
    }
    RecordBatch::try_new(schema, columns)
}

fn adapt_array(array: &ArrayRef, target: &DataType, copied_buffer_bytes: &mut u64)
    -> Result<ArrayRef, ArrowError> {
    match (array.data_type(), target) {
        (DataType::List(_), DataType::FixedSizeList(field, dimension)) => {
            let list = array.as_any().downcast_ref::<ListArray>()
                .ok_or_else(|| invalid("Expected a variable list array"))?;
            fixed_vector(list, field, *dimension, copied_buffer_bytes)
        }
        (DataType::List(_), DataType::List(field)) => {
            let list = array.as_any().downcast_ref::<ListArray>()
                .ok_or_else(|| invalid("Expected a variable list array"))?;
            Ok(Arc::new(ListArray::try_new(field.clone(), list.offsets().clone(),
                adapt_array(list.values(), field.data_type(), copied_buffer_bytes)?, list.nulls().cloned())?))
        }
        (DataType::Struct(source), DataType::Struct(fields)) => {
            if source.len() != fields.len() || source.iter().zip(fields)
                .any(|(left, right)| left.name() != right.name()) {
                return Err(invalid("Physical nested struct fields differ from task output"));
            }
            let row = array.as_any().downcast_ref::<StructArray>()
                .ok_or_else(|| invalid("Expected a struct array"))?;
            let columns = row.columns().iter().zip(fields).map(|(column, field)|
                adapt_array(column, field.data_type(), copied_buffer_bytes)).collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(StructArray::try_new(fields.clone(), columns, row.nulls().cloned())?))
        }
        (DataType::Map(_, sorted), DataType::Map(field, expected_sorted)) if sorted == expected_sorted => {
            let map = array.as_any().downcast_ref::<MapArray>()
                .ok_or_else(|| invalid("Expected a map array"))?;
            let entries: ArrayRef = Arc::new(map.entries().clone());
            let adapted = adapt_array(&entries, field.data_type(), copied_buffer_bytes)?;
            let entries = adapted.as_any().downcast_ref::<StructArray>()
                .ok_or_else(|| invalid("Physical map entries must remain a complete struct"))?;
            Ok(Arc::new(MapArray::try_new(field.clone(), map.offsets().clone(), entries.clone(),
                map.nulls().cloned(), *expected_sorted)?))
        }
        (source, expected) if source == expected => Ok(array.clone()),
        _ => Err(invalid("Physical column type differs from task output; coercion is forbidden")),
    }
}

fn fixed_vector(list: &ListArray, field: &FieldRef, dimension: i32, copied_buffer_bytes: &mut u64)
    -> Result<ArrayRef, ArrowError> {
    if dimension <= 0 {
        return Err(invalid("Physical VECTOR dimension must be positive"));
    }
    if list.values().data_type() != field.data_type() {
        return Err(invalid("Physical VECTOR element type differs from task output; coercion is forbidden"));
    }
    let offsets = list.value_offsets();
    if offsets.windows(2).all(|pair| pair[1] - pair[0] == dimension) {
        let length = list.len().checked_mul(dimension as usize)
            .ok_or_else(|| invalid("Physical VECTOR element count overflow"))?;
        // Sliced lists retain their actual first offset. NULL parents also need
        // exactly dimension slots to share the payload without synthesizing data.
        let values = list.values().slice(offsets[0] as usize, length);
        return Ok(Arc::new(FixedSizeListArray::try_new(field.clone(), dimension, values,
            list.nulls().cloned())?));
    }
    let mut slices = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) {
            // A NULL vector is distinct from a valid vector containing NULL elements.
            slices.push(new_null_array(field.data_type(), dimension as usize));
        } else {
            let value = list.value(row);
            if value.len() != dimension as usize {
                return Err(invalid("Physical VECTOR dimension differs from a non-NULL input row"));
            }
            slices.push(adapt_array(&value, field.data_type(), copied_buffer_bytes)?);
        }
    }
    let values = if slices.is_empty() {
        new_null_array(field.data_type(), 0)
    } else {
        let result = concat(&slices.iter().map(|array| array.as_ref()).collect::<Vec<_>>())?;
        *copied_buffer_bytes = copied_buffer_bytes.checked_add(buffer_bytes(&result.to_data())?)
            .ok_or_else(|| invalid("Physical adaptation copy-byte count overflow"))?;
        result
    };
    Ok(Arc::new(FixedSizeListArray::try_new(field.clone(), dimension, values,
        list.nulls().cloned())?))
}

fn buffer_bytes(data: &arrow::array::ArrayData) -> Result<u64, ArrowError> {
    let mut bytes = 0u64;
    for buffer in data.buffers() {
        bytes = bytes.checked_add(buffer.len() as u64)
            .ok_or_else(|| invalid("Physical adaptation buffer-byte count overflow"))?;
    }
    if let Some(nulls) = data.nulls() {
        bytes = bytes.checked_add(nulls.buffer().len() as u64)
            .ok_or_else(|| invalid("Physical adaptation buffer-byte count overflow"))?;
    }
    for child in data.child_data() {
        bytes = bytes.checked_add(buffer_bytes(child)?)
            .ok_or_else(|| invalid("Physical adaptation buffer-byte count overflow"))?;
    }
    Ok(bytes)
}

fn invalid(message: &str) -> ArrowError {
    ArrowError::InvalidArgumentError(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::types::{Float32Type, Float64Type};
    use arrow::array::{Float64Array, Int64Array};
    use arrow_schema::{Field, Schema};

    #[test]
    fn sliced_vectors_preserve_double_precision_null_parents_and_null_elements() {
        let vectors = ListArray::from_iter_primitive::<Float64Type, _, _>([
            Some(vec![Some(99.0), Some(98.0), Some(97.0)]),
            Some(vec![Some(1.0000000001), None, Some(-4.0)]),
            None,
            Some(vec![Some(3.0), Some(4.0), Some(5.0)]),
        ]).slice(1, 3);
        let target = Arc::new(Field::new("element", DataType::Float64, true));
        let mut copied = 0;
        let actual = fixed_vector(&vectors, &target, 3, &mut copied).unwrap();
        let actual = actual.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let first = actual.value(0);
        let first = first.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(first.value(0), 1.0000000001);
        assert!(first.is_null(1));
        assert!(!actual.is_null(0));
        assert!(actual.is_null(1));
        let last = actual.value(2);
        let last = last.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(last.values().as_ref(), &[3.0, 4.0, 5.0]);
        assert_ne!(actual.values().to_data().buffers()[0].as_ptr(),
            vectors.values().to_data().buffers()[0].as_ptr());
        assert_eq!(copied, buffer_bytes(&actual.values().to_data()).unwrap());
        assert!(copied > 0);
    }

    #[test]
    fn continuous_sliced_vectors_share_actual_payload_and_preserve_null_masks() {
        let element = Arc::new(Field::new("item", DataType::Float64, true));
        let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(99.0), Some(98.0), Some(97.0),
            Some(1.0000000001), None, Some(-4.0), None, None, None,
            Some(3.0), Some(4.0), Some(5.0)]));
        let vectors = ListArray::try_new(element,
            arrow::buffer::OffsetBuffer::new(vec![0i32, 3, 6, 9, 12].into()), values.clone(),
            Some(arrow::buffer::NullBuffer::from(vec![true, true, false, true]))).unwrap().slice(1, 3);
        let target = Arc::new(Field::new("element", DataType::Float64, true));
        let mut copied = 0;
        let actual = fixed_vector(&vectors, &target, 3, &mut copied).unwrap();
        let actual = actual.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let payload = actual.values().as_any().downcast_ref::<Float64Array>().unwrap();
        let source = values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(payload.values().as_ptr(), source.values().as_ptr().wrapping_add(3));
        assert_eq!(payload.value(0), 1.0000000001);
        assert!(payload.is_null(1));
        assert!(!actual.is_null(0));
        assert!(actual.is_null(1));
        assert!(!actual.is_null(2));
        assert_eq!(&payload.values()[6..], &[3.0, 4.0, 5.0]);
        assert_eq!(copied, 0);
        assert!(fixed_vector(&vectors, &Arc::new(Field::new("element", DataType::Float64, false)),
            3, &mut 0).is_err());
    }

    #[test]
    fn shared_null_parent_slots_do_not_make_required_elements_nullable() {
        let target = Arc::new(Field::new("element", DataType::Float64, false));
        let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0000000001), Some(-4.0),
            None, None, Some(3.0), Some(4.0)]));
        let vectors = ListArray::try_new(Arc::new(Field::new("item", DataType::Float64, true)),
            arrow::buffer::OffsetBuffer::new(vec![0i32, 2, 4, 6].into()), values.clone(),
            Some(arrow::buffer::NullBuffer::from(vec![true, false, true]))).unwrap();
        let mut copied = 0;
        let actual = fixed_vector(&vectors, &target, 2, &mut copied).unwrap();
        let actual = actual.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(actual.values().to_data().buffers()[0].as_ptr(), values.to_data().buffers()[0].as_ptr());
        assert!(actual.is_null(1));
        assert!(!actual.is_null(0));
        assert!(!actual.is_null(2));
        assert_eq!(copied, 0);
    }

    #[test]
    fn invalid_dimension_and_element_precision_are_rejected_not_coerced_or_nullified() {
        let wrong = ListArray::from_iter_primitive::<Float32Type, _, _>([
            Some(vec![Some(1.0), Some(2.0)])]);
        assert!(fixed_vector(&wrong, &Arc::new(Field::new("item", DataType::Float32, true)), 3, &mut 0).is_err());
        let correct = ListArray::from_iter_primitive::<Float32Type, _, _>([
            Some(vec![Some(1.0), Some(2.0), Some(3.0)])]);
        assert!(fixed_vector(&correct, &Arc::new(Field::new("item", DataType::Float64, true)), 3, &mut 0).is_err());
        let non_nullable = ListArray::from_iter_primitive::<Float32Type, _, _>([
            Some(vec![Some(1.0), None, Some(3.0)]), None]);
        assert!(fixed_vector(&non_nullable,
            &Arc::new(Field::new("item", DataType::Float32, false)), 3, &mut 0).is_err());
    }

    #[test]
    fn unrelated_nested_lists_and_all_struct_fields_are_preserved() {
        let list = ListArray::from_iter_primitive::<Float32Type, _, _>([
            Some(vec![Some(1.0)]), None, Some(vec![Some(2.0), Some(3.0)])]);
        let source = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false),
            Field::new("items", list.data_type().clone(), true)]));
        let batch = RecordBatch::try_new(source.clone(), vec![
            Arc::new(Int64Array::from(vec![5, 6, 7])), Arc::new(list)]).unwrap();
        let mut copied = 0;
        let actual = adapt_batch(&batch, source, &mut copied).unwrap();
        assert_eq!(copied, 0);
        assert_eq!(actual.num_columns(), 2);
        assert_eq!(actual.column(0).to_data(), batch.column(0).to_data());
        assert_eq!(actual.column(1).to_data(), batch.column(1).to_data());
        let nullable = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false),
            Field::new("items", batch.column(1).data_type().clone(), false)]));
        assert!(adapt_batch(&batch, nullable, &mut copied).is_err());
        let entries = Arc::new(StructArray::try_new(batch.schema().fields().clone(),
            batch.columns().to_vec(), None).unwrap());
        let nested_schema = Arc::new(Schema::new(vec![Field::new("records", entries.data_type().clone(), true)]));
        let nested = RecordBatch::try_new(nested_schema.clone(), vec![entries]).unwrap();
        let retained = adapt_batch(&nested, nested_schema, &mut copied).unwrap();
        let row = retained.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(row.num_columns(), 2);
        assert_eq!(row.column(0).to_data(), batch.column(0).to_data());
        assert_eq!(row.column(1).to_data(), batch.column(1).to_data());
    }

    #[test]
    fn zero_column_nonempty_input_is_refused_without_silently_losing_rows() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(schema.clone(), Vec::new(),
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(3))).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert!(adapt_batch(&batch, schema, &mut 0).is_err());
    }

    #[test]
    fn actual_copies_remain_cumulative_when_a_later_field_fails() {
        let vector: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([
            Some(vec![Some(1.0000000001), Some(2.0)]), None,
            Some(vec![Some(3.0), Some(4.0)]),
        ]));
        let source = Arc::new(Schema::new(vec![
            Field::new("embedding", vector.data_type().clone(), true),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(source, vec![vector, Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        let good = Arc::new(Schema::new(vec![
            Field::new("embedding", DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float64, true)), 2), true),
            Field::new("id", DataType::Int64, false),
        ]));
        let mut usage = crate::canonical::pangrova::protocol::v2::ResourceUsage::default();
        let actual = adapt_batch(&batch, good.clone(), &mut usage.columnar_conversion_copied_bytes).unwrap();
        let vector = actual.column(0).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let one_copy = buffer_bytes(&vector.values().to_data()).unwrap();
        assert!(one_copy > 0);
        assert_eq!(usage.columnar_conversion_copied_bytes, one_copy);
        let wrong = Arc::new(Schema::new(vec![good.field(0).clone(), Field::new("id", DataType::Utf8, false)]));
        assert!(adapt_batch(&batch, wrong, &mut usage.columnar_conversion_copied_bytes).is_err());
        assert_eq!(usage.columnar_conversion_copied_bytes, one_copy * 2);
        // A validation refusal before any copy cannot erase observations.
        assert!(adapt_batch(&batch, Arc::new(Schema::empty()),
            &mut usage.columnar_conversion_copied_bytes).is_err());
        assert_eq!(usage.columnar_conversion_copied_bytes, one_copy * 2);
    }
}

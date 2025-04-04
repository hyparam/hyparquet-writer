import { CompressionCodec, ConvertedType, Encoding, FieldRepetitionType, PageType, ParquetType } from 'hyparquet/src/constants.js'
import { serializeTCompactProtocol } from './thrift.js'
import { unconvertMetadata } from './unconvert.js'

/**
 * @import {FileMetaData} from 'hyparquet'
 * @import {Writer} from './writer.js'
 * @param {Writer} writer
 * @param {FileMetaData} metadata
 */
export function writeMetadata(writer, metadata) {
  const compact = {
    field_1: metadata.version,
    field_2: metadata.schema && metadata.schema.map(element => ({
      field_1: element.type && ParquetType.indexOf(element.type),
      field_2: element.type_length,
      field_3: element.repetition_type && FieldRepetitionType.indexOf(element.repetition_type),
      field_4: element.name,
      field_5: element.num_children,
      field_6: element.converted_type && ConvertedType.indexOf(element.converted_type),
      field_7: element.scale,
      field_8: element.precision,
      field_9: element.field_id,
      field_10: element.logical_type,
    })),
    field_3: metadata.num_rows,
    field_4: metadata.row_groups.map(rg => ({
      field_1: rg.columns.map((c, columnIndex) => ({
        field_1: c.file_path,
        field_2: c.file_offset,
        field_3: c.meta_data && {
          field_1: ParquetType.indexOf(c.meta_data.type),
          field_2: c.meta_data.encodings.map(e => Encoding.indexOf(e)),
          field_3: c.meta_data.path_in_schema,
          field_4: CompressionCodec.indexOf(c.meta_data.codec),
          field_5: c.meta_data.num_values,
          field_6: c.meta_data.total_uncompressed_size,
          field_7: c.meta_data.total_compressed_size,
          field_8: c.meta_data.key_value_metadata,
          field_9: c.meta_data.data_page_offset,
          field_10: c.meta_data.index_page_offset,
          field_11: c.meta_data.dictionary_page_offset,
          field_12: c.meta_data.statistics && {
            field_1: unconvertMetadata(c.meta_data.statistics.max, metadata.schema[columnIndex + 1]),
            field_2: unconvertMetadata(c.meta_data.statistics.min, metadata.schema[columnIndex + 1]),
            field_3: c.meta_data.statistics.null_count,
            field_4: c.meta_data.statistics.distinct_count,
            field_5: unconvertMetadata(c.meta_data.statistics.max_value, metadata.schema[columnIndex + 1]),
            field_6: unconvertMetadata(c.meta_data.statistics.min_value, metadata.schema[columnIndex + 1]),
            field_7: c.meta_data.statistics.is_max_value_exact,
            field_8: c.meta_data.statistics.is_min_value_exact,
          },
          field_13: c.meta_data.encoding_stats && c.meta_data.encoding_stats.map(es => ({
            field_1: PageType.indexOf(es.page_type),
            field_2: Encoding.indexOf(es.encoding),
            field_3: es.count,
          })),
          field_14: c.meta_data.bloom_filter_offset,
          field_15: c.meta_data.bloom_filter_length,
          field_16: c.meta_data.size_statistics && {
            field_1: c.meta_data.size_statistics.unencoded_byte_array_data_bytes,
            field_2: c.meta_data.size_statistics.repetition_level_histogram,
            field_3: c.meta_data.size_statistics.definition_level_histogram,
          },
        },
        field_4: c.offset_index_offset,
        field_5: c.offset_index_length,
        field_6: c.column_index_offset,
        field_7: c.column_index_length,
        field_8: c.crypto_metadata,
        field_9: c.encrypted_column_metadata,
      })),
      field_2: rg.total_byte_size,
      field_3: rg.num_rows,
      field_4: rg.sorting_columns && rg.sorting_columns.map(sc => ({
        field_1: sc.column_idx,
        field_2: sc.descending,
        field_3: sc.nulls_first,
      })),
      field_5: rg.file_offset,
      field_6: rg.total_compressed_size,
      // field_7: rg.ordinal, // should be int16
    })),
    field_5: metadata.key_value_metadata && metadata.key_value_metadata.map(kv => ({
      field_1: kv.key,
      field_2: kv.value,
    })),
    field_6: metadata.created_by,
  }

  const metadataStart = writer.offset
  serializeTCompactProtocol(writer, compact)
  const metadataLength = writer.offset - metadataStart
  writer.appendUint32(metadataLength)
}

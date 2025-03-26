import { Encoding, ParquetType } from 'hyparquet/src/constants.js'
import { serializeTCompactProtocol } from './thrift.js'

const CompressionCodec = [
  'UNCOMPRESSED',
  'SNAPPY',
  'GZIP',
  'LZO',
  'BROTLI',
  'LZ4',
  'ZSTD',
  'LZ4_RAW',
]

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
      field_3: element.repetition_type,
      field_4: element.name,
      field_5: element.num_children,
      field_6: element.converted_type,
      field_7: element.scale,
      field_8: element.precision,
      field_9: element.field_id,
      field_10: element.logical_type,
    })),
    field_3: metadata.num_rows,
    field_4: metadata.row_groups.map(rg => ({
      field_1: rg.columns.map(c => ({
        field_1: c.file_path,
        field_2: c.file_offset,
        field_3: c.meta_data && {
          field_1: ParquetType.indexOf(c.meta_data.type),
          field_2: c.meta_data.encodings.map(e => Encoding.indexOf(e)), // WTF simplfy?
          field_3: c.meta_data.path_in_schema,
          field_4: CompressionCodec.indexOf(c.meta_data.codec),
          field_5: c.meta_data.num_values,
          field_6: c.meta_data.total_uncompressed_size,
          field_7: c.meta_data.total_compressed_size,
          field_8: c.meta_data.key_value_metadata,
          field_9: c.meta_data.data_page_offset,
          field_10: c.meta_data.index_page_offset,
          field_11: c.meta_data.dictionary_page_offset,
          field_12: c.meta_data.statistics,
          field_13: c.meta_data.encoding_stats,
          field_14: c.meta_data.bloom_filter_offset,
          field_15: c.meta_data.bloom_filter_length,
          field_16: c.meta_data.size_statistics,
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
      field_4: rg.sorting_columns,
      field_5: rg.file_offset,
      field_6: rg.total_compressed_size,
      field_7: rg.ordinal,
    })),
    field_5: metadata.key_value_metadata,
    field_6: metadata.created_by,
  }

  const metadataStart = writer.offset
  serializeTCompactProtocol(writer, compact)
  const metadataLength = writer.offset - metadataStart
  writer.appendUint32(metadataLength)
}

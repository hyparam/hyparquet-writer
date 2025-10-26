import { getSchemaPath } from 'hyparquet/src/schema.js'
import { CompressionCodec, ConvertedType, Encoding, FieldRepetitionType, PageType, ParquetType } from 'hyparquet/src/constants.js'
import { serializeTCompactProtocol } from './thrift.js'
import { unconvertStatistics } from './unconvert.js'

/**
 * @import {FileMetaData, LogicalType, TimeUnit} from 'hyparquet'
 * @import {ThriftObject, Writer} from '../src/types.js'
 * @param {Writer} writer
 * @param {FileMetaData} metadata
 */
export function writeMetadata(writer, metadata) {
  /** @type {ThriftObject} */
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
      field_10: logicalType(element.logical_type),
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
          field_8: c.meta_data.key_value_metadata && c.meta_data.key_value_metadata.map(kv => ({
            field_1: kv.key,
            field_2: kv.value,
          })),
          field_9: c.meta_data.data_page_offset,
          field_10: c.meta_data.index_page_offset,
          field_11: c.meta_data.dictionary_page_offset,
          field_12: c.meta_data.statistics && unconvertStatistics(
            c.meta_data.statistics,
            schemaElement(metadata.schema, c.meta_data.path_in_schema, columnIndex + 1)
          ),
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
          field_17: c.meta_data.geospatial_statistics && {
            field_1: c.meta_data.geospatial_statistics.bbox && {
              field_1: c.meta_data.geospatial_statistics.bbox.xmin,
              field_2: c.meta_data.geospatial_statistics.bbox.xmax,
              field_3: c.meta_data.geospatial_statistics.bbox.ymin,
              field_4: c.meta_data.geospatial_statistics.bbox.ymax,
              field_5: c.meta_data.geospatial_statistics.bbox.zmin,
              field_6: c.meta_data.geospatial_statistics.bbox.zmax,
              field_7: c.meta_data.geospatial_statistics.bbox.mmin,
              field_8: c.meta_data.geospatial_statistics.bbox.mmax,
            },
            field_2: c.meta_data.geospatial_statistics.geospatial_types,
          },
        },
        field_4: c.offset_index_offset,
        field_5: c.offset_index_length,
        field_6: c.column_index_offset,
        field_7: c.column_index_length,
        // field_8: c.crypto_metadata,
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

  // write metadata as thrift
  const metadataStart = writer.offset
  serializeTCompactProtocol(writer, compact)
  // write metadata length
  const metadataLength = writer.offset - metadataStart
  writer.appendUint32(metadataLength)
}

/**
 * Resolve schema element for statistics using the stored path.
 *
 * @param {import('hyparquet').SchemaElement[]} schema
 * @param {string[] | undefined} path
 * @param {number} fallbackIndex
 * @returns {import('hyparquet').SchemaElement}
 */
function schemaElement(schema, path, fallbackIndex) {
  if (path?.length) {
    const resolved = getSchemaPath(schema, path).at(-1)?.element
    if (resolved) return resolved
  }
  return schema[fallbackIndex]
}

/**
 * @param {LogicalType | undefined} type
 * @returns {ThriftObject | undefined}
 */
export function logicalType(type) {
  if (!type) return
  if (type.type === 'STRING') return { field_1: {} }
  if (type.type === 'MAP') return { field_2: {} }
  if (type.type === 'LIST') return { field_3: {} }
  if (type.type === 'ENUM') return { field_4: {} }
  if (type.type === 'DECIMAL') return { field_5: {
    field_1: type.scale,
    field_2: type.precision,
  } }
  if (type.type === 'DATE') return { field_6: {} }
  if (type.type === 'TIME') return { field_7: {
    field_1: type.isAdjustedToUTC,
    field_2: timeUnit(type.unit),
  } }
  if (type.type === 'TIMESTAMP') return { field_8: {
    field_1: type.isAdjustedToUTC,
    field_2: timeUnit(type.unit),
  } }
  if (type.type === 'INTEGER') return { field_10: {
    field_1: type.bitWidth,
    field_2: type.isSigned,
  } }
  if (type.type === 'NULL') return { field_11: {} }
  if (type.type === 'JSON') return { field_12: {} }
  if (type.type === 'BSON') return { field_13: {} }
  if (type.type === 'UUID') return { field_14: {} }
  if (type.type === 'FLOAT16') return { field_15: {} }
  if (type.type === 'VARIANT') return { field_16: {} }
  if (type.type === 'GEOMETRY') return { field_17: {
    field_1: type.crs,
  } }
  if (type.type === 'GEOGRAPHY') return { field_18: {
    field_1: type.crs,
    field_2: type.algorithm && edgeAlgorithm[type.algorithm],
  } }
}

/**
 * @param {TimeUnit} unit
 * @returns {ThriftObject}
 */
function timeUnit(unit) {
  if (unit === 'NANOS') return { field_3: {} }
  if (unit === 'MICROS') return { field_2: {} }
  return { field_1: {} }
}

/**
 * @import {EdgeInterpolationAlgorithm} from 'hyparquet/src/types.js'
 * @type {Record<EdgeInterpolationAlgorithm, number>}
 */
const edgeAlgorithm = {
  SPHERICAL: 0,
  VINCENTY: 1,
  THOMAS: 2,
  ANDOYER: 3,
  KARNEY: 4,
}

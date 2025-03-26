import { Encoding, PageType } from 'hyparquet/src/constants.js'
import { writeRleBitPackedHybrid } from './encoding.js'
import { writePlain } from './plain.js'
import { serializeTCompactProtocol } from './thrift.js'
import { Writer } from './writer.js'

/**
 * @import {ColumnMetaData, DecodedArray, PageHeader, ParquetType} from 'hyparquet/src/types.js'
 * @param {Writer} writer
 * @param {string} columnName
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @returns {ColumnMetaData}
 */
export function writeColumn(writer, columnName, values, type) {
  // Get data stats
  const num_nulls = values.filter(v => v === null).length
  const offsetStart = writer.offset

  // Write page to temp buffer
  const page = new Writer()

  /** @type {import('hyparquet/src/types.js').Encoding} */
  const encoding = 'PLAIN'

  // TODO: repetition levels
  const maxRepetitionLevel = 0
  let repetition_levels_byte_length = 0
  if (maxRepetitionLevel) {
    repetition_levels_byte_length = writeRleBitPackedHybrid(page, [])
  }

  // TODO: definition levels
  const maxDefinitionLevel = 0
  let definition_levels_byte_length = 0
  if (maxDefinitionLevel) {
    definition_levels_byte_length = writeRleBitPackedHybrid(page, [])
  }

  // write page data (TODO: compressed)
  const { uncompressed_page_size, compressed_page_size } = writePageData(page, values, type)

  // write page header
  /** @type {PageHeader} */
  const header = {
    type: 'DATA_PAGE_V2',
    uncompressed_page_size,
    compressed_page_size,
    data_page_header_v2: {
      num_values: values.length,
      num_nulls,
      num_rows: values.length,
      encoding,
      definition_levels_byte_length,
      repetition_levels_byte_length,
      is_compressed: false,
    },
  }
  writePageHeader(writer, header)

  // write page data
  writer.appendBuffer(page.getBuffer())

  return {
    type,
    encodings: ['PLAIN'],
    path_in_schema: [columnName],
    codec: 'UNCOMPRESSED',
    num_values: BigInt(values.length),
    total_compressed_size: BigInt(writer.offset - offsetStart),
    total_uncompressed_size: BigInt(writer.offset - offsetStart),
    data_page_offset: BigInt(offsetStart),
  }
}

/**
 * Deduce a ParquetType from the JS value
 *
 * @param {DecodedArray} values
 * @returns {ParquetType}
 */
export function getParquetTypeForValues(values) {
  if (values.every(v => typeof v === 'boolean')) return 'BOOLEAN'
  if (values.every(v => typeof v === 'bigint')) return 'INT64'
  if (values.every(v => Number.isInteger(v))) return 'INT32'
  if (values.every(v => typeof v === 'number')) return 'DOUBLE'
  if (values.every(v => typeof v === 'string')) return 'BYTE_ARRAY'
  throw new Error(`Cannot determine parquet type for: ${values}`)
}

/**
 * @param {Writer} writer
 * @param {PageHeader} header
 */
function writePageHeader(writer, header) {
  const compact = {
    field_1: PageType.indexOf(header.type),
    field_2: header.uncompressed_page_size,
    field_3: header.compressed_page_size,
    field_8: header.data_page_header_v2 && {
      field_1: header.data_page_header_v2.num_values,
      field_2: header.data_page_header_v2.num_nulls,
      field_3: header.data_page_header_v2.num_rows,
      field_4: Encoding.indexOf(header.data_page_header_v2.encoding),
      field_5: header.data_page_header_v2.definition_levels_byte_length,
      field_6: header.data_page_header_v2.repetition_levels_byte_length,
      field_7: header.data_page_header_v2.is_compressed ? undefined : false, // default true
    },
  }
  serializeTCompactProtocol(writer, compact)
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @returns {{ uncompressed_page_size: number, compressed_page_size: number }}
 */
function writePageData(writer, values, type) {
  // write plain data
  const startOffset = writer.offset
  writePlain(writer, values, type)
  const size = writer.offset - startOffset

  return { uncompressed_page_size: size, compressed_page_size: size }
}

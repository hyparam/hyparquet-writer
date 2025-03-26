import { Encoding, PageType } from 'hyparquet/src/constants.js'
import { writeRleBitPackedHybrid } from './encoding.js'
import { writePlain } from './plain.js'
import { serializeTCompactProtocol } from './thrift.js'
import { Writer } from './writer.js'

/**
 * @import {ColumnMetaData, DecodedArray, FieldRepetitionType, PageHeader, ParquetType, SchemaElement} from 'hyparquet/src/types.js'
 * @param {Writer} writer
 * @param {SchemaElement[]} schemaPath schema path for the column
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @returns {ColumnMetaData}
 */
export function writeColumn(writer, schemaPath, values, type) {
  const offsetStart = writer.offset
  let num_nulls = 0

  // Write page to temp buffer
  const page = new Writer()

  /** @type {import('hyparquet/src/types.js').Encoding} */
  const encoding = 'PLAIN'

  // TODO: repetition levels
  const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
  let repetition_levels_byte_length = 0
  if (maxRepetitionLevel) {
    repetition_levels_byte_length = writeRleBitPackedHybrid(page, [])
  }

  // TODO: definition levels
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
  let definition_levels_byte_length = 0
  if (maxDefinitionLevel) {
    const definitionLevels = []
    for (const value of values) {
      if (value === null || value === undefined) {
        definitionLevels.push(maxDefinitionLevel - 1)
        num_nulls++
      } else {
        definitionLevels.push(maxDefinitionLevel)
      }
    }
    definition_levels_byte_length = writeRleBitPackedHybrid(page, definitionLevels)
  }

  // write page data
  writePageData(page, values, type)

  // TODO: compress page data

  // write page header
  /** @type {PageHeader} */
  const header = {
    type: 'DATA_PAGE_V2',
    uncompressed_page_size: page.offset,
    compressed_page_size: page.offset,
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
    path_in_schema: schemaPath.slice(1).map(s => s.name),
    codec: 'UNCOMPRESSED',
    num_values: BigInt(values.length),
    total_compressed_size: BigInt(writer.offset - offsetStart),
    total_uncompressed_size: BigInt(writer.offset - offsetStart),
    data_page_offset: BigInt(offsetStart),
  }
}

/**
 * Deduce a ParquetType from JS values
 *
 * @param {DecodedArray} values
 * @returns {{ type: ParquetType, repetition_type: 'REQUIRED' | 'OPTIONAL' }}
 */
export function getParquetTypeForValues(values) {
  if (values instanceof Int32Array) return { type: 'INT32', repetition_type: 'REQUIRED' }
  if (values instanceof BigInt64Array) return { type: 'INT64', repetition_type: 'REQUIRED' }
  if (values instanceof Float32Array) return { type: 'FLOAT', repetition_type: 'REQUIRED' }
  if (values instanceof Float64Array) return { type: 'DOUBLE', repetition_type: 'REQUIRED' }
  /** @type {ParquetType | undefined} */
  let type = undefined
  /** @type {FieldRepetitionType} */
  let repetition_type = 'REQUIRED'
  for (const value of values) {
    const valueType = getParquetTypeForValue(value)
    if (!valueType) {
      repetition_type = 'OPTIONAL'
    } else if (type === undefined) {
      type = valueType
    } else if (type === 'INT32' && valueType === 'DOUBLE') {
      type = 'DOUBLE'
    } else if (type === 'DOUBLE' && valueType === 'INT32') {
      // keep
    } else if (type !== valueType) {
      throw new Error(`parquet cannot write mixed types: ${type} and ${valueType}`)
    }
  }
  if (!type) throw new Error('parquetWrite: empty column cannot determine type')
  return { type, repetition_type }
}

/**
 * @param {any} value
 * @returns {ParquetType | undefined}
 */
function getParquetTypeForValue(value) {
  if (value === null || value === undefined) return undefined
  if (value === true || value === false) return 'BOOLEAN'
  if (typeof value === 'bigint') return 'INT64'
  if (Number.isInteger(value)) return 'INT32'
  if (typeof value === 'number') return 'DOUBLE'
  if (typeof value === 'string') return 'BYTE_ARRAY'
  throw new Error(`Cannot determine parquet type for: ${value}`)
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
 */
function writePageData(writer, values, type) {
  // write plain data
  writePlain(writer, values, type)
}

/**
 * Get the max repetition level for a given schema path.
 *
 * @param {SchemaElement[]} schemaPath
 * @returns {number} max repetition level
 */
function getMaxRepetitionLevel(schemaPath) {
  let maxLevel = 0
  for (const element of schemaPath) {
    if (element.repetition_type === 'REPEATED') {
      maxLevel++
    }
  }
  return maxLevel
}

/**
 * Get the max definition level for a given schema path.
 *
 * @param {SchemaElement[]} schemaPath
 * @returns {number} max definition level
 */
function getMaxDefinitionLevel(schemaPath) {
  let maxLevel = 0
  for (const element of schemaPath.slice(1)) {
    if (element.repetition_type !== 'REQUIRED') {
      maxLevel++
    }
  }
  return maxLevel
}

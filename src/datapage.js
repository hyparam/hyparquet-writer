import { Encodings, PageTypes } from 'hyparquet/src/constants.js'
import { ByteWriter } from './bytewriter.js'
import { deltaBinaryPack, deltaByteArray, deltaLengthByteArray } from './delta.js'
import { writeRleBitPackedHybrid } from './encoding.js'
import { writePlain } from './plain.js'
import { getMaxRepetitionLevel } from './schema.js'
import { writeByteStreamSplit } from './splitstream.js'
import { serializeTCompactProtocol } from './thrift.js'

/**
 * @param {Object} options
 * @param {Writer} options.writer
 * @param {DecodedArray} options.values
 * @param {ColumnEncoder} options.column
 * @param {Encoding} options.encoding
 * @param {PageData} options.pageData
 */
export function writeDataPageV2({ writer, values, column, encoding, pageData }) {
  const { columnName, element, codec, compressors } = column
  const { type, type_length, repetition_type } = element

  if (!type) throw new Error(`column ${columnName} cannot determine type`)
  if (repetition_type === 'REPEATED') throw new Error(`column ${columnName} repeated types not supported`)

  // write levels to temp buffer
  const levelWriter = new ByteWriter()
  const {
    definition_levels_byte_length,
    repetition_levels_byte_length,
    num_nulls,
    num_values,
    num_rows,
  } = writeLevels(levelWriter, column, pageData)

  const nonnull = values.filter(v => v !== null && v !== undefined)

  // write page data to temp buffer
  const page = new ByteWriter()
  if (encoding === 'PLAIN') {
    writePlain(page, nonnull, type, type_length)
  } else if (encoding === 'RLE') {
    if (type !== 'BOOLEAN') throw new Error('RLE encoding only supported for BOOLEAN type')
    const rleData = new ByteWriter()
    writeRleBitPackedHybrid(rleData, nonnull, 1)
    page.appendUint32(rleData.offset) // prepend byte length
    page.appendBuffer(rleData.getBuffer())
  } else if (encoding === 'PLAIN_DICTIONARY' || encoding === 'RLE_DICTIONARY') {
    // find max bitwidth
    let maxValue = 0
    for (const v of values) if (v > maxValue) maxValue = v
    const bitWidth = Math.ceil(Math.log2(maxValue + 1))
    page.appendUint8(bitWidth) // prepend bitWidth
    writeRleBitPackedHybrid(page, nonnull, bitWidth)
  } else if (encoding === 'DELTA_BINARY_PACKED') {
    if (type !== 'INT32' && type !== 'INT64') {
      throw new Error('DELTA_BINARY_PACKED encoding only supported for INT32 and INT64 types')
    }
    deltaBinaryPack(page, nonnull)
  } else if (encoding === 'DELTA_LENGTH_BYTE_ARRAY') {
    if (type !== 'BYTE_ARRAY') {
      throw new Error('DELTA_LENGTH_BYTE_ARRAY encoding only supported for BYTE_ARRAY type')
    }
    deltaLengthByteArray(page, nonnull)
  } else if (encoding === 'DELTA_BYTE_ARRAY') {
    if (type !== 'BYTE_ARRAY') {
      throw new Error('DELTA_BYTE_ARRAY encoding only supported for BYTE_ARRAY type')
    }
    deltaByteArray(page, nonnull)
  } else if (encoding === 'BYTE_STREAM_SPLIT') {
    writeByteStreamSplit(page, nonnull, type, type_length)
  } else {
    throw new Error(`parquet unsupported encoding: ${encoding}`)
  }

  // compress page data
  const pageBuffer = new Uint8Array(page.getBuffer())
  const compressedBytes = compressors[codec]?.(pageBuffer) ?? pageBuffer

  // write page header
  writePageHeader(writer, {
    type: 'DATA_PAGE_V2',
    uncompressed_page_size: levelWriter.offset + page.offset,
    compressed_page_size: levelWriter.offset + compressedBytes.length,
    data_page_header_v2: {
      num_values,
      num_nulls,
      num_rows,
      encoding,
      definition_levels_byte_length,
      repetition_levels_byte_length,
      is_compressed: !!codec,
    },
  })

  // write levels
  writer.appendBuffer(levelWriter.getBuffer())

  // write page data
  writer.appendBytes(compressedBytes)
}

/**
 * @param {Writer} writer
 * @param {PageHeader} header
 */
export function writePageHeader(writer, header) {
  /** @type {ThriftObject} */
  const compact = {
    field_1: PageTypes.indexOf(header.type),
    field_2: header.uncompressed_page_size,
    field_3: header.compressed_page_size,
    field_4: header.crc,
    field_5: header.data_page_header && {
      field_1: header.data_page_header.num_values,
      field_2: Encodings.indexOf(header.data_page_header.encoding),
      field_3: Encodings.indexOf(header.data_page_header.definition_level_encoding),
      field_4: Encodings.indexOf(header.data_page_header.repetition_level_encoding),
      // field_5: header.data_page_header.statistics,
    },
    field_7: header.dictionary_page_header && {
      field_1: header.dictionary_page_header.num_values,
      field_2: Encodings.indexOf(header.dictionary_page_header.encoding),
    },
    field_8: header.data_page_header_v2 && {
      field_1: header.data_page_header_v2.num_values,
      field_2: header.data_page_header_v2.num_nulls,
      field_3: header.data_page_header_v2.num_rows,
      field_4: Encodings.indexOf(header.data_page_header_v2.encoding),
      field_5: header.data_page_header_v2.definition_levels_byte_length,
      field_6: header.data_page_header_v2.repetition_levels_byte_length,
      field_7: header.data_page_header_v2.is_compressed ? undefined : false, // default true
    },
  }
  serializeTCompactProtocol(writer, compact)
}

/**
 * @import {DecodedArray, Encoding, PageHeader} from 'hyparquet'
 * @import {ColumnEncoder, PageData, ThriftObject, Writer} from '../src/types.js'
 * @param {Writer} writer
 * @param {ColumnEncoder} column
 * @param {PageData} dataPage
 * @returns {{
 *   definition_levels_byte_length: number
 *   repetition_levels_byte_length: number
 *   num_nulls: number
 *   num_values: number
 *   num_rows: number
 * }}
 */
function writeLevels(writer, column, dataPage) {
  const { schemaPath } = column
  const { values, definitionLevels, repetitionLevels, numNulls, maxDefinitionLevel } = dataPage
  const num_rows = repetitionLevels.length ? repetitionLevels.reduce((n, r) => r === 0 ? n + 1 : n, 0) : values.length
  const num_values = definitionLevels.length || values.length

  const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
  let repetition_levels_byte_length = 0
  if (maxRepetitionLevel) {
    const bitWidth = Math.ceil(Math.log2(maxRepetitionLevel + 1))
    repetition_levels_byte_length = writeRleBitPackedHybrid(writer, repetitionLevels, bitWidth)
  }

  let definition_levels_byte_length = 0
  if (maxDefinitionLevel) {
    const bitWidth = Math.ceil(Math.log2(maxDefinitionLevel + 1))
    definition_levels_byte_length = writeRleBitPackedHybrid(writer, definitionLevels, bitWidth)
  }
  return { definition_levels_byte_length, repetition_levels_byte_length, num_nulls: numNulls, num_values, num_rows }
}

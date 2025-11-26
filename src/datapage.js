import { Encodings, PageTypes } from 'hyparquet/src/constants.js'
import { ByteWriter } from './bytewriter.js'
import { deltaBinaryPack, deltaByteArray, deltaLengthByteArray } from './delta.js'
import { writeRleBitPackedHybrid } from './encoding.js'
import { writePlain } from './plain.js'
import { snappyCompress } from './snappy.js'
import { serializeTCompactProtocol } from './thrift.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel } from './schema.js'

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 * @param {ColumnEncoder} column
 * @param {Encoding} encoding
 * @param {PageData} [listValues]
 */
export function writeDataPageV2(writer, values, column, encoding, listValues) {
  const { columnName, element, compressed } = column
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
  } = writeLevels(levelWriter, column, values, listValues)

  const nonnull = values.filter(v => v !== null && v !== undefined)

  // write page data to temp buffer
  const page = new ByteWriter()
  if (encoding === 'PLAIN') {
    writePlain(page, nonnull, type, type_length)
  } else if (encoding === 'RLE') {
    if (type !== 'BOOLEAN') throw new Error('RLE encoding only supported for BOOLEAN type')
    page.appendUint32(nonnull.length) // prepend length
    writeRleBitPackedHybrid(page, nonnull, 1)
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
  } else {
    throw new Error(`parquet unsupported encoding: ${encoding}`)
  }

  // compress page data
  let compressedPage = page
  if (compressed) {
    compressedPage = new ByteWriter()
    snappyCompress(compressedPage, new Uint8Array(page.getBuffer()))
  }

  // write page header
  writePageHeader(writer, {
    type: 'DATA_PAGE_V2',
    uncompressed_page_size: levelWriter.offset + page.offset,
    compressed_page_size: levelWriter.offset + compressedPage.offset,
    data_page_header_v2: {
      num_values,
      num_nulls,
      num_rows: values.length,
      encoding,
      definition_levels_byte_length,
      repetition_levels_byte_length,
      is_compressed: compressed,
    },
  })

  // write levels
  writer.appendBuffer(levelWriter.getBuffer())

  // write page data
  writer.appendBuffer(compressedPage.getBuffer())
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
 * @param {DecodedArray} values
 * @param {PageData} [listValues]
 * @returns {{
 *   definition_levels_byte_length: number
 *   repetition_levels_byte_length: number
 *   num_nulls: number
 *   num_values: number
 * }}
 */
function writeLevels(writer, column, values, listValues) {
  const { schemaPath } = column
  const definitionLevels = listValues?.definitionLevels
  const repetitionLevels = listValues?.repetitionLevels

  let num_nulls = listValues?.numNulls ?? 0
  let num_values = definitionLevels?.length ?? values.length

  const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
  let repetition_levels_byte_length = 0
  if (maxRepetitionLevel) {
    const bitWidth = Math.ceil(Math.log2(maxRepetitionLevel + 1))
    const reps = repetitionLevels ?? []
    repetition_levels_byte_length = writeRleBitPackedHybrid(writer, reps, bitWidth)
  }

  // definition levels
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
  let definition_levels_byte_length = 0
  if (maxDefinitionLevel) {
    const bitWidth = Math.ceil(Math.log2(maxDefinitionLevel + 1))
    const defs = definitionLevels ?? (() => {
      const generated = []
      for (const value of values) {
        if (value === null || value === undefined) {
          generated.push(maxDefinitionLevel - 1)
          num_nulls++
        } else {
          generated.push(maxDefinitionLevel)
        }
      }
      num_values = generated.length
      return generated
    })()

    if (definitionLevels && listValues === undefined) {
      num_nulls = definitionLevels.reduce(
        (count, def) => def === maxDefinitionLevel ? count : count + 1,
        0
      )
    }

    definition_levels_byte_length = writeRleBitPackedHybrid(writer, defs, bitWidth)
  } else {
    num_nulls = values.filter(value => value === null || value === undefined).length
  }
  return { definition_levels_byte_length, repetition_levels_byte_length, num_nulls, num_values }
}

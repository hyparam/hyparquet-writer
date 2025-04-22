import { Encoding, PageType } from 'hyparquet/src/constants.js'
import { ByteWriter } from './bytewriter.js'
import { writeRleBitPackedHybrid } from './encoding.js'
import { writePlain } from './plain.js'
import { snappyCompress } from './snappy.js'
import { serializeTCompactProtocol } from './thrift.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel } from './schema.js'

/**
 * @import {Writer} from '../src/types.js'
 * @param {Writer} writer
 * @param {DecodedArray} values
 * @param {SchemaElement[]} schemaPath
 * @param {import('hyparquet').Encoding} encoding
 * @param {boolean} compressed
 */
export function writeDataPageV2(writer, values, schemaPath, encoding, compressed) {
  const { name, type, type_length, repetition_type } = schemaPath[schemaPath.length - 1]

  if (!type) throw new Error(`column ${name} cannot determine type`)
  if (repetition_type === 'REPEATED') throw new Error(`column ${name} repeated types not supported`)

  // write levels to temp buffer
  const levels = new ByteWriter()
  const { definition_levels_byte_length, repetition_levels_byte_length, num_nulls }
     = writeLevels(levels, schemaPath, values)

  const nonnull = values.filter(v => v !== null && v !== undefined)

  // write page data to temp buffer
  const page = new ByteWriter()
  if (encoding === 'RLE') {
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
  } else {
    writePlain(page, nonnull, type, type_length)
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
    uncompressed_page_size: levels.offset + page.offset,
    compressed_page_size: levels.offset + compressedPage.offset,
    data_page_header_v2: {
      num_values: values.length,
      num_nulls,
      num_rows: values.length,
      encoding,
      definition_levels_byte_length,
      repetition_levels_byte_length,
      is_compressed: compressed,
    },
  })

  // write levels
  writer.appendBuffer(levels.getBuffer())

  // write page data
  writer.appendBuffer(compressedPage.getBuffer())
}

/**
 * @param {Writer} writer
 * @param {PageHeader} header
 */
export function writePageHeader(writer, header) {
  /** @type {import('../src/types.js').ThriftObject} */
  const compact = {
    field_1: PageType.indexOf(header.type),
    field_2: header.uncompressed_page_size,
    field_3: header.compressed_page_size,
    field_4: header.crc,
    field_5: header.data_page_header && {
      field_1: header.data_page_header.num_values,
      field_2: Encoding.indexOf(header.data_page_header.encoding),
      field_3: Encoding.indexOf(header.data_page_header.definition_level_encoding),
      field_4: Encoding.indexOf(header.data_page_header.repetition_level_encoding),
      // field_5: header.data_page_header.statistics,
    },
    field_7: header.dictionary_page_header && {
      field_1: header.dictionary_page_header.num_values,
      field_2: Encoding.indexOf(header.dictionary_page_header.encoding),
    },
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
 * @import {DecodedArray, PageHeader, SchemaElement} from 'hyparquet'
 * @param {Writer} writer
 * @param {SchemaElement[]} schemaPath
 * @param {DecodedArray} values
 * @returns {{ definition_levels_byte_length: number, repetition_levels_byte_length: number, num_nulls: number}}
 */
function writeLevels(writer, schemaPath, values) {
  let num_nulls = 0

  // TODO: repetition levels
  const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
  let repetition_levels_byte_length = 0
  if (maxRepetitionLevel) {
    repetition_levels_byte_length = writeRleBitPackedHybrid(writer, [], 0)
  }

  // definition levels
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
    const bitWidth = Math.ceil(Math.log2(maxDefinitionLevel + 1))
    definition_levels_byte_length = writeRleBitPackedHybrid(writer, definitionLevels, bitWidth)
  }
  return { definition_levels_byte_length, repetition_levels_byte_length, num_nulls }
}

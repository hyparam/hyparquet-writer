import { Encoding, PageType } from 'hyparquet/src/constants.js'
import { unconvert } from './convert.js'
import { writeRleBitPackedHybrid } from './encoding.js'
import { writePlain } from './plain.js'
import { serializeTCompactProtocol } from './thrift.js'
import { Writer } from './writer.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel } from './schema.js'

/**
 * @import {ColumnMetaData, DecodedArray, PageHeader, ParquetType, SchemaElement} from 'hyparquet'
 * @param {Writer} writer
 * @param {SchemaElement[]} schemaPath
 * @param {DecodedArray} values
 * @returns {ColumnMetaData}
 */
export function writeColumn(writer, schemaPath, values) {
  const schemaElement = schemaPath[schemaPath.length - 1]
  const { type } = schemaElement
  if (!type) throw new Error(`column ${schemaElement.name} cannot determine type`)
  const offsetStart = writer.offset
  let num_nulls = 0

  // Unconvert type if necessary
  values = unconvert(schemaElement, values)

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

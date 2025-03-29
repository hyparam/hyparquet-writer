
/**
 * Deduce a ParquetType from JS values
 *
 * @import {ConvertedType, DecodedArray, FieldRepetitionType, ParquetType, SchemaElement} from 'hyparquet'
 * @param {string} name
 * @param {DecodedArray} values
 * @param {ParquetType | undefined} type
 * @returns {SchemaElement}
 */
export function getSchemaElementForValues(name, values, type) {
  if (values instanceof Int32Array) return { name, type: 'INT32', repetition_type: 'REQUIRED' }
  if (values instanceof BigInt64Array) return { name, type: 'INT64', repetition_type: 'REQUIRED' }
  if (values instanceof Float32Array) return { name, type: 'FLOAT', repetition_type: 'REQUIRED' }
  if (values instanceof Float64Array) return { name, type: 'DOUBLE', repetition_type: 'REQUIRED' }
  /** @type {FieldRepetitionType} */
  let repetition_type = 'REQUIRED'
  /** @type {ConvertedType | undefined} */
  let converted_type = undefined
  for (const value of values) {
    if (value === null || value === undefined) {
      repetition_type = 'OPTIONAL'
    } else {
      // value is defined
      /** @type {ParquetType | undefined} */
      let valueType = undefined
      if (value === true || value === false) valueType = 'BOOLEAN'
      else if (typeof value === 'bigint') valueType = 'INT64'
      else if (Number.isInteger(value)) valueType = 'INT32'
      else if (typeof value === 'number') valueType = 'DOUBLE'
      else if (value instanceof Uint8Array) valueType = 'BYTE_ARRAY'
      else if (typeof value === 'string') {
        valueType = 'BYTE_ARRAY'
        // make sure they are all strings
        if (type && !converted_type) throw new Error('mixed types not supported')
        converted_type = 'UTF8'
      }
      else if (value instanceof Date) {
        valueType = 'INT64'
        // make sure they are all dates
        if (type && !converted_type) throw new Error('mixed types not supported')
        converted_type = 'TIMESTAMP_MILLIS'
      }
      else if (typeof value === 'object') {
        // use json (TODO: native list and object types)
        converted_type = 'JSON'
        valueType = 'BYTE_ARRAY'
      }
      else if (!valueType) throw new Error(`cannot determine parquet type for: ${value}`)

      // expand type if necessary
      if (type === undefined) {
        type = valueType
      } else if (type === 'INT32' && valueType === 'DOUBLE') {
        type = 'DOUBLE'
      } else if (type === 'DOUBLE' && valueType === 'INT32') {
        // keep
      } else if (type !== valueType) {
        throw new Error(`parquet cannot write mixed types: ${type} and ${valueType}`)
      }
    }
  }
  if (!type) throw new Error(`column ${name} cannot determine type`)
  return { name, type, repetition_type, converted_type }
}

/**
 * Get the max repetition level for a given schema path.
 *
 * @param {SchemaElement[]} schemaPath
 * @returns {number} max repetition level
 */
export function getMaxRepetitionLevel(schemaPath) {
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
export function getMaxDefinitionLevel(schemaPath) {
  let maxLevel = 0
  for (const element of schemaPath.slice(1)) {
    if (element.repetition_type !== 'REQUIRED') {
      maxLevel++
    }
  }
  return maxLevel
}

/**
 * Infer a schema from column data.
 * Accepts optional schemaOverrides to override the type of columns by name.
 *
 * @param {object} options
 * @param {ColumnSource[]} options.columnData
 * @param {Record<string,SchemaElement>} [options.schemaOverrides]
 * @returns {SchemaElement[]}
 */
export function schemaFromColumnData({ columnData, schemaOverrides }) {
  /** @type {SchemaElement[]} */
  const schema = [{
    name: 'root',
    num_children: columnData.length,
  }]
  let num_rows = 0

  for (const { name, data, type, nullable } of columnData) {
    // check if all columns have the same length
    num_rows = num_rows || data.length
    if (num_rows !== data.length) {
      throw new Error('columns must have the same length')
    }

    if (schemaOverrides?.[name]) {
      // use schema override
      const override = schemaOverrides[name]
      if (override.name !== name) throw new Error('schema override name does not match column name')
      if (override.num_children) throw new Error('schema override cannot have children')
      if (override.repetition_type === 'REPEATED') throw new Error('schema override cannot be repeated')
      schema.push(override)
    } else if (type) {
      // use provided type
      schema.push(basicTypeToSchemaElement(name, type, nullable))
    } else {
      // auto-detect type
      schema.push(autoSchemaElement(name, data))
    }
  }

  return schema
}

/**
 * @import {ConvertedType, DecodedArray, FieldRepetitionType, ParquetType, SchemaElement} from 'hyparquet'
 * @import {BasicType, ColumnSource} from '../src/types.js'
 * @param {string} name
 * @param {BasicType} type
 * @param {boolean} [nullable]
 * @returns {SchemaElement}
 */
function basicTypeToSchemaElement(name, type, nullable) {
  const repetition_type = nullable === false ? 'REQUIRED' : 'OPTIONAL'
  if (type === 'STRING') {
    return { name, type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type }
  }
  if (type === 'JSON') {
    return { name, type: 'BYTE_ARRAY', converted_type: 'JSON', repetition_type }
  }
  if (type === 'TIMESTAMP') {
    return { name, type: 'INT64', converted_type: 'TIMESTAMP_MILLIS', repetition_type }
  }
  if (type === 'UUID') {
    return { name, type: 'FIXED_LEN_BYTE_ARRAY', type_length: 16, logical_type: { type: 'UUID' }, repetition_type }
  }
  if (type === 'FLOAT16') {
    return { name, type: 'FIXED_LEN_BYTE_ARRAY', type_length: 2, logical_type: { type: 'FLOAT16' }, repetition_type }
  }
  return { name, type, repetition_type }
}

/**
 * Automatically determine a SchemaElement from an array of values.
 *
 * @param {string} name
 * @param {DecodedArray} values
 * @returns {SchemaElement}
 */
export function autoSchemaElement(name, values) {
  /** @type {ParquetType | undefined} */
  let type
  /** @type {FieldRepetitionType} */
  let repetition_type = 'REQUIRED'
  /** @type {ConvertedType | undefined} */
  let converted_type = undefined

  if (values instanceof Int32Array) return { name, type: 'INT32', repetition_type }
  if (values instanceof BigInt64Array) return { name, type: 'INT64', repetition_type }
  if (values instanceof Float32Array) return { name, type: 'FLOAT', repetition_type }
  if (values instanceof Float64Array) return { name, type: 'DOUBLE', repetition_type }

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
        valueType = 'DOUBLE'
      }
      if (type !== valueType) {
        throw new Error(`parquet cannot write mixed types: ${type} and ${valueType}`)
      }
    }
  }
  if (!type) {
    // fallback to nullable BYTE_ARRAY
    // TODO: logical_type: 'NULL'
    type = 'BYTE_ARRAY'
    repetition_type = 'OPTIONAL'
  }
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

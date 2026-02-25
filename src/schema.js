/**
 * @import {ConvertedType, DecodedArray, FieldRepetitionType, ParquetType, SchemaElement} from 'hyparquet'
 * @import {BasicType, ColumnSource} from '../src/types.js'
 */

/**
 * Infer a schema from column data.
 * Accepts optional schemaOverrides to override the type of columns by name.
 *
 * @param {object} options
 * @param {ColumnSource[]} options.columnData
 * @param {Record<string, SchemaElement>} [options.schemaOverrides]
 * @returns {SchemaElement[]}
 */
export function schemaFromColumnData({ columnData, schemaOverrides }) {
  /** @type {SchemaElement[]} */
  const schema = [{
    name: 'root',
    num_children: columnData.length,
  }]

  for (const { name, data, type, nullable } of columnData) {
    if (schemaOverrides?.[name]) {
      // use schema override
      const override = schemaOverrides[name]
      if (type || nullable !== undefined) {
        throw new Error(`cannot provide both type and schema override for column ${name}`)
      }
      if (override.name !== name) {
        throw new Error(`schema override for column ${name} must have matching name, got ${override.name}`)
      }
      if (override.type === 'FIXED_LEN_BYTE_ARRAY' && !override.type_length) {
        throw new Error('schema override for FIXED_LEN_BYTE_ARRAY must include type_length')
      }
      // TODO: support nested schema overrides
      if (override.num_children) {
        throw new Error('schema override does not support nested types')
      }
      schema.push(override)
    } else if (type) {
      // use provided type
      schema.push(basicTypeToSchemaElement(name, type, nullable))
    } else {
      // auto-detect type from first 1000 values
      schema.push(autoSchemaElement(name, data.slice(0, 1000)))
    }
  }

  return schema
}

/**
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
  if (type === 'GEOMETRY') {
    return { name, type: 'BYTE_ARRAY', logical_type: { type: 'GEOMETRY' }, repetition_type }
  }
  if (type === 'GEOGRAPHY') {
    return { name, type: 'BYTE_ARRAY', logical_type: { type: 'GEOGRAPHY' }, repetition_type }
  }
  return { name, type, repetition_type }
}

/**
 * Automatically determine a SchemaElement from an array of values.
 *
 * @param {string} name the column name
 * @param {DecodedArray} values the column values
 * @returns {SchemaElement}
 */
export function autoSchemaElement(name, values) {
  /** @type {ParquetType | undefined} */
  let type
  /** @type {FieldRepetitionType} */
  let repetition_type = 'REQUIRED'
  /** @type {ConvertedType | undefined} */
  let converted_type

  if (values instanceof Int32Array) return { name, type: 'INT32', repetition_type }
  if (values instanceof BigInt64Array) return { name, type: 'INT64', repetition_type }
  if (values instanceof Float32Array) return { name, type: 'FLOAT', repetition_type }
  if (values instanceof Float64Array) return { name, type: 'DOUBLE', repetition_type }

  for (const value of values) {
    if (value === null || value === undefined) {
      repetition_type = 'OPTIONAL'
    } else {
      // value is defined, infer type
      /** @type {ParquetType} */
      let valueType
      /** @type {ConvertedType | undefined} */
      let valueConvertedType
      if (typeof value === 'boolean') valueType = 'BOOLEAN'
      else if (typeof value === 'bigint') valueType = 'INT64'
      else if (Number.isInteger(value)) valueType = 'INT32'
      else if (typeof value === 'number') valueType = 'DOUBLE'
      else if (value instanceof Uint8Array) valueType = 'BYTE_ARRAY'
      else if (typeof value === 'string') {
        valueType = 'BYTE_ARRAY'
        valueConvertedType = 'UTF8'
      }
      else if (value instanceof Date) {
        valueType = 'INT64'
        valueConvertedType = 'TIMESTAMP_MILLIS'
      }
      else if (typeof value === 'object') {
        // use json (TODO: native list and object types)
        valueType = 'BYTE_ARRAY'
        valueConvertedType = 'JSON'
      }
      else throw new Error(`cannot determine parquet type for: ${value}`)

      // expand type if necessary
      if (type === undefined) {
        type = valueType
        converted_type = valueConvertedType
      } else if (type === 'INT32' && valueType === 'DOUBLE') {
        type = 'DOUBLE'
      } else if (type === 'DOUBLE' && valueType === 'INT32') {
        valueType = 'DOUBLE'
      } else if (type !== valueType || converted_type !== valueConvertedType) {
        throw new Error(`parquet cannot write mixed types: ${converted_type ?? type} and ${valueConvertedType ?? valueType}`)
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

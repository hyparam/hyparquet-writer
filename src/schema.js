/**
 * Convert column data to schema.
 *
 * @param {import('./types.js').ColumnData[]} columnData
 * @returns {import('./types.js').SchemaElement[]}
 */
export function schemaFromColumnData(columnData) {
  /** @type {import('./types.js').SchemaElement[]} */
  const schema = [{
    name: 'root',
    num_children: columnData.length,
  }]
  let num_rows = 0

  for (const column of columnData) {
    // check if all columns have the same length
    num_rows = num_rows || column.data.length
    if (num_rows !== column.data.length) {
      throw new Error('columns must have the same length')
    }

    const { data, ...schemaElement } = column
    if (column.type) {
      // use provided type
      schema.push(schemaElement)
    } else {
      // auto-detect type
      schema.push(autoSchemaElement(column.name, data))
    }
  }

  return schema
}

/**
 * Deduce a ParquetType from JS values
 *
 * @param {string} name
 * @param {import('./types.js').DecodedArray} values
 * @returns {import('./types.js').SchemaElement}
 */
function autoSchemaElement(name, values) {
  /** @type {import('./types.js').ParquetType | undefined} */
  let type
  /** @type {import('hyparquet').FieldRepetitionType} */
  let repetition_type = 'REQUIRED'
  /** @type {import('hyparquet').ConvertedType | undefined} */
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
      /** @type {import('./types.js').ParquetType | undefined} */
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
  if (!type) throw new Error(`column ${name} cannot determine type`)
  return { name, type, repetition_type, converted_type }
}

/**
 * Get the max repetition level for a given schema path.
 *
 * @param {import('./types.js').SchemaElement[]} schemaPath
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
 * @param {import('./types.js').SchemaElement[]} schemaPath
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

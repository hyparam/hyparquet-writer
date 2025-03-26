
/**
 * Convert from rich to primitive types.
 *
 * @import {DecodedArray, SchemaElement} from 'hyparquet'
 * @param {SchemaElement} schemaElement
 * @param {DecodedArray} values
 * @returns {DecodedArray}
 */
export function unconvert(schemaElement, values) {
  const ctype = schemaElement.converted_type
  if (ctype === 'DATE') {
    return values.map(v => v.getTime())
  }
  if (ctype === 'JSON') {
    const encoder = new TextEncoder()
    if (!Array.isArray(values)) throw new Error('JSON must be an array')
    return values.map(v => encoder.encode(JSON.stringify(v)))
  }
  return values
}

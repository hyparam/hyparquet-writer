
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
    if (!Array.isArray(values)) throw new Error('JSON must be an array')
    const encoder = new TextEncoder()
    return values.map(v => encoder.encode(JSON.stringify(v)))
  }
  if (ctype === 'UTF8') {
    if (!Array.isArray(values)) throw new Error('strings must be an array')
    const encoder = new TextEncoder()
    return values.map(v => encoder.encode(v))
  }
  return values
}

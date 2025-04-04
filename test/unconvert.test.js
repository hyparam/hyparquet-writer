import { describe, expect, it } from 'vitest'
import { unconvert } from '../src/unconvert.js'

/**
 * @import {SchemaElement} from 'hyparquet'
 */
describe('unconvert', () => {
  it('should return Date objects when converted_type = DATE', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', converted_type: 'DATE' }
    const input = [new Date('2020-01-01T00:00:00Z'), new Date('2021-01-01T00:00:00Z')]
    const result = unconvert(schema, input)

    expect(result).toEqual([
      new Date('2020-01-01T00:00:00Z').getTime(),
      new Date('2021-01-01T00:00:00Z').getTime(),
    ])
  })

  it('should convert JSON objects to strings when converted_type = JSON', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', converted_type: 'JSON' }
    const input = [{ foo: 'bar' }, { hello: 'world' }]
    const result = unconvert(schema, input)

    // We check that result is an array of Uint8Arrays containing the JSON-encoded bytes
    expect(result).toHaveLength(2)
    expect(result[0]).toBeInstanceOf(Uint8Array)
    expect(new TextDecoder().decode(result[0])).toEqual(JSON.stringify({ foo: 'bar' }))
    expect(new TextDecoder().decode(result[1])).toEqual(JSON.stringify({ hello: 'world' }))
  })

  it('should convert string array to Uint8Array when converted_type = UTF8', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', converted_type: 'UTF8' }
    const input = ['hello', 'world']
    const result = unconvert(schema, input)

    expect(result).toHaveLength(2)
    expect(result[0]).toBeInstanceOf(Uint8Array)
    expect(new TextDecoder().decode(result[0])).toBe('hello')
    expect(new TextDecoder().decode(result[1])).toBe('world')
  })

  it('should throw an error when converted_type = UTF8 and values is not an array', () => {
    expect(() => unconvert(
      { name: 'test', converted_type: 'UTF8' },
      new Uint8Array([1, 2, 3]))
    ).toThrow('strings must be an array')
  })

  it('should throw an error when converted_type = JSON and values is not an array', () => {
    expect(() => unconvert(
      { name: 'test', converted_type: 'JSON' },
      new Uint8Array([1, 2, 3]))
    ).toThrow('JSON must be an array')
  })

  it('should return original values if there is no recognized converted_type', () => {
    const input = [1, 2, 3]
    const result = unconvert({ name: 'test' }, input)
    expect(result).toEqual(input)
  })
})

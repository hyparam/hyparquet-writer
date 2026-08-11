import { describe, expect, it } from 'vitest'
import { ByteWriter } from '../src/bytewriter.js'
import { estimateValueSize, useDictionary, writeDictionaryPage } from '../src/dictionary.js'
import { writePlain } from '../src/plain.js'

/**
 * FNV-1a hash, mirroring the byte-array bucketing in src/dictionary.js. Used
 * here only to assert that the chosen collision pair genuinely collides.
 * @param {Uint8Array} bytes
 * @returns {number}
 */
function hashBytes(bytes) {
  let h = 0x811c9dc5
  for (let i = 0; i < bytes.length; i++) {
    h ^= bytes[i]
    h = Math.imul(h, 0x01000193)
  }
  return h >>> 0
}

describe('estimateValueSize', () => {
  it('returns 0 for null and undefined', () => {
    expect(estimateValueSize(null, 'INT32')).toBe(0)
    expect(estimateValueSize(undefined, 'BYTE_ARRAY')).toBe(0)
  })

  it('returns fixed sizes for primitive types', () => {
    expect(estimateValueSize(true, 'BOOLEAN')).toBe(0.125)
    expect(estimateValueSize(1, 'INT32')).toBe(4)
    expect(estimateValueSize(1.5, 'FLOAT')).toBe(4)
    expect(estimateValueSize(1n, 'INT64')).toBe(8)
    expect(estimateValueSize(1.5, 'DOUBLE')).toBe(8)
    expect(estimateValueSize(1n, 'INT96')).toBe(12)
  })

  it('uses type_length for FIXED_LEN_BYTE_ARRAY', () => {
    expect(estimateValueSize(new Uint8Array(4), 'FIXED_LEN_BYTE_ARRAY', 16)).toBe(16)
    expect(estimateValueSize(new Uint8Array(4), 'FIXED_LEN_BYTE_ARRAY')).toBe(0)
  })

  it('estimates BYTE_ARRAY by byte or character length', () => {
    expect(estimateValueSize(new Uint8Array(7), 'BYTE_ARRAY')).toBe(7)
    expect(estimateValueSize('hello', 'BYTE_ARRAY')).toBe(5)
    expect(estimateValueSize('🙂', 'BYTE_ARRAY')).toBe(2)
    expect(estimateValueSize(42, 'BYTE_ARRAY')).toBe(0) // neither bytes nor string
  })
})

describe('useDictionary', () => {
  it('dedupes repeated strings', () => {
    const { dictionary, indexes } = useDictionary(['xx', 'xx', 'xx', 'xx', 'yy'], 'BYTE_ARRAY', undefined, undefined, 0)
    expect(dictionary).toEqual(['xx', 'yy'])
    expect(indexes).toEqual([0, 0, 0, 0, 1])
  })

  it('dedupes repeated numbers', () => {
    const { dictionary, indexes } = useDictionary([1, 1, 2, 2, 1], 'INT32', undefined, undefined, 0)
    expect(dictionary).toEqual([1, 2])
    expect(indexes).toEqual([0, 0, 1, 1, 0])
  })

  it('falls back (returns {}) when values are mostly unique', () => {
    expect(useDictionary(['a', 'b', 'c'], 'BYTE_ARRAY', undefined, undefined, 0)).toEqual({})
  })

  it('falls back when a non-dictionary encoding is requested', () => {
    expect(useDictionary(['x', 'x'], 'BYTE_ARRAY', undefined, 'PLAIN', 0)).toEqual({})
  })

  it('proceeds when RLE_DICTIONARY is explicitly requested', () => {
    const { dictionary } = useDictionary([1, 1, 1, 1], 'INT32', undefined, 'RLE_DICTIONARY', 0)
    expect(dictionary).toEqual([1])
  })

  it('falls back for BOOLEAN', () => {
    expect(useDictionary([true, true, true], 'BOOLEAN', undefined, undefined, 0)).toEqual({})
  })

  it('dedupes byte arrays by content, not object identity', () => {
    function blob() { return Uint8Array.of(9, 8, 7) }
    const { dictionary, indexes } = useDictionary([blob(), blob(), blob(), blob()], 'BYTE_ARRAY', undefined, undefined, 0)
    expect(dictionary).toEqual([blob()])
    expect(indexes).toEqual([0, 0, 0, 0])
  })

  it('returns the original byte-array objects in the dictionary', () => {
    const first = Uint8Array.of(1, 2, 3)
    const { dictionary } = useDictionary([first, Uint8Array.of(1, 2, 3)], 'BYTE_ARRAY', undefined, undefined, 0)
    expect(dictionary).toHaveLength(1)
    expect(dictionary?.[0]).toBe(first) // same reference, not a copy
  })

  it('keeps hash-colliding byte arrays as distinct entries', () => {
    // these two byte sequences share an FNV-1a hash but differ
    function a() { return Uint8Array.of(27, 83, 52, 67, 82, 108, 98, 124) }
    function b() { return Uint8Array.of(25, 73, 7, 10, 109, 25, 4, 10) }
    expect(hashBytes(a())).toBe(hashBytes(b()))
    expect(a()).not.toEqual(b())

    const { dictionary, indexes } = useDictionary(
      [a(), b(), a(), b(), a(), b()], 'BYTE_ARRAY', undefined, undefined, 0
    )
    expect(dictionary).toEqual([a(), b()])
    expect(indexes).toEqual([0, 1, 0, 1, 0, 1])
  })

  it('skips null/undefined values but keeps their slots', () => {
    function blob() { return Uint8Array.of(5, 5) }
    const { dictionary, indexes } = useDictionary([blob(), blob(), blob(), blob(), null], 'BYTE_ARRAY', undefined, undefined, 0)
    expect(dictionary).toEqual([blob()])
    expect(indexes).toEqual([0, 0, 0, 0, undefined]) // null slot left empty
  })

  it('rejects null values in required columns', () => {
    expect(() => useDictionary([1, 1, null], 'INT32', undefined, undefined, 0, true))
      .toThrow('parquet required value is undefined')
  })

  it('falls back when the dictionary would exceed the dictionarySize cap', () => {
    // three distinct 50-byte blobs cycled; low cardinality clears the sample
    // check, but cumulative dictionary size (150) exceeds the explicit cap (120)
    function a() { return new Uint8Array(50).fill(1) }
    function b() { return new Uint8Array(50).fill(2) }
    function c() { return new Uint8Array(50).fill(3) }
    const data = []
    for (let i = 0; i < 30; i++) data.push([a, b, c][i % 3]())
    expect(useDictionary(data, 'BYTE_ARRAY', undefined, undefined, 120)).toEqual({})
  })

  it('uses UTF-8 bytes only to enforce dictionarySize', () => {
    const value = '🙂'.repeat(10)
    const data = new Array(10).fill(value)
    expect(useDictionary(data, 'BYTE_ARRAY', undefined, undefined, 30)).toEqual({})
    expect(useDictionary(data, 'BYTE_ARRAY', undefined, undefined, 40).dictionary).toEqual([value])
  })

  it('keeps a large dictionary when it at least halves the bytes', () => {
    // 100 distinct 20kb strings, 20 repeats each: dictionary is 2mb but total
    // values are 40mb, a 20x win, so it must be kept
    const distinct = Array.from({ length: 100 }, (_, i) => String(i).padStart(8, '0').repeat(2500))
    const data = []
    for (let r = 0; r < 20; r++) for (const v of distinct) data.push(v)
    const { dictionary, indexes } = useDictionary(data, 'BYTE_ARRAY', undefined, undefined, undefined)
    expect(dictionary?.length).toBe(100)
    expect(indexes?.length).toBe(2000)
  })

  it('falls back when a large dictionary does not halve the bytes', () => {
    // first 1000 values are 100 distinct 20kb strings (sample ratio 0.1), then
    // 1000 unique 20kb strings: dictionary is 22mb of 40mb total, less than a
    // 2x win, so plain encoding is the better trade
    const distinct = Array.from({ length: 100 }, (_, i) => String(i).padStart(8, '0').repeat(2500))
    const data = []
    for (let r = 0; r < 10; r++) for (const v of distinct) data.push(v)
    for (let i = 0; i < 1000; i++) data.push(String(i + 1000).padStart(8, '0').repeat(2500))
    expect(useDictionary(data, 'BYTE_ARRAY', undefined, undefined, undefined)).toEqual({})
  })

  it('falls back when a small dictionary does not halve the bytes', () => {
    expect(useDictionary(['a', 'a', 'b', 'c'], 'BYTE_ARRAY', undefined, undefined, undefined)).toEqual({})
  })

  it('falls back for a losing dictionary with mixed BYTE_ARRAY values', () => {
    const bytes = Array.from(
      { length: 850 },
      (_, i) => Uint8Array.of(i >> 8, i & 0xff)
    )
    const data = [...bytes, ...new Array(150).fill('x')]
    expect(useDictionary(data, 'BYTE_ARRAY', undefined, undefined, undefined)).toEqual({})
  })

  it('counts BYTE_ARRAY length prefixes when evaluating short strings', () => {
    const distinct = Array.from({ length: 100 }, (_, i) => String(i).padStart(2, '0'))
    const data = Array.from({ length: 1000 }, (_, i) => distinct[i % distinct.length])
    const { dictionary, indexes } = useDictionary(data, 'BYTE_ARRAY', undefined, undefined, undefined)
    expect(dictionary).toEqual(distinct)
    expect(indexes).toHaveLength(data.length)
  })

  it('counts BYTE_ARRAY length prefixes when evaluating empty byte arrays', () => {
    const data = Array.from({ length: 1000 }, () => new Uint8Array())
    const { dictionary, indexes } = useDictionary(data, 'BYTE_ARRAY', undefined, undefined, undefined)
    expect(dictionary).toEqual([new Uint8Array()])
    expect(indexes).toHaveLength(data.length)
  })

  it('includes dictionary indexes in the win check', () => {
    const data = Array.from({ length: 20_000 }, (_, i) => i % 9_990)
    expect(useDictionary(data, 'INT32', undefined, undefined, undefined)).toEqual({})
  })

  it('samples evenly across phase-changing values', () => {
    const uniquePrefix = Array.from({ length: 1000 }, (_, i) => `unique-${i}`)
    const repeatedTail = new Array(9000).fill('repeated')
    const { dictionary } = useDictionary(
      [...uniquePrefix, ...repeatedTail], 'BYTE_ARRAY', undefined, undefined, undefined
    )
    expect(dictionary).toHaveLength(1001)
  })

  it('builds a winning dictionary when sampled distinct count exceeds 50%', () => {
    const distinct = Array.from({ length: 800 }, (_, i) => String(i).padStart(8, '0').repeat(8))
    const data = Array.from({ length: 4000 }, (_, i) => distinct[i % distinct.length])
    const { dictionary, indexes } = useDictionary(data, 'BYTE_ARRAY', undefined, undefined, undefined)
    expect(dictionary).toHaveLength(800)
    expect(indexes).toHaveLength(4000)
  })

  it('builds a dictionary unconditionally when RLE_DICTIONARY is forced', () => {
    // all-unique values fail the sample check, but a forced encoding must be
    // honored so the written pages match the declared encoding
    const { dictionary, indexes } = useDictionary(['a', 'b', 'c'], 'BYTE_ARRAY', undefined, 'RLE_DICTIONARY', undefined)
    expect(dictionary).toEqual(['a', 'b', 'c'])
    expect(indexes).toEqual([0, 1, 2])
  })
})

describe('writeDictionaryPage', () => {
  /**
   * @param {import('hyparquet').ParquetType | undefined} type
   * @param {number} [type_length]
   * @returns {any}
   */
  function column(type, type_length) {
    return {
      columnName: 'col',
      element: { name: 'col', type, type_length },
      codec: 'UNCOMPRESSED',
      compressors: {},
    }
  }

  it('writes a dictionary page header followed by the plain-encoded values', () => {
    const dictionary = [Uint8Array.of(1, 2, 3), Uint8Array.of(4, 5)]
    const writer = new ByteWriter()
    writeDictionaryPage(writer, column('BYTE_ARRAY'), dictionary)
    const out = writer.getBytes()

    // uncompressed: page body equals the plain encoding of the dictionary
    const body = new ByteWriter()
    writePlain(body, dictionary, 'BYTE_ARRAY', undefined)
    const expected = body.getBytes()

    expect(out.length).toBeGreaterThan(expected.length) // header precedes body
    expect(out.slice(out.length - expected.length)).toEqual(expected)
  })

  it('throws when the column type cannot be determined', () => {
    expect(() => writeDictionaryPage(new ByteWriter(), column(undefined), [Uint8Array.of(1)]))
      .toThrow('cannot determine type')
  })
})

import { describe, expect, it } from 'vitest'
import { hashParquetValue, readBloomFilter } from 'hyparquet/src/bloom.js'
import { xxhash64 } from 'hyparquet/src/xxhash.js'
import { ByteWriter } from '../src/bytewriter.js'
import { BloomBuilder, createBloomFilter, optimalNumBytes, sbbfContains, sbbfInsert, writeBloomFilter } from '../src/bloom.js'

const textEncoder = new TextEncoder()

describe('optimalNumBytes', () => {
  it('returns the minimum 32 bytes for tiny NDV', () => {
    expect(optimalNumBytes(1, 0.01)).toBe(32)
  })

  it('grows with NDV and snaps below 1024 to a power of two', () => {
    const small = optimalNumBytes(100, 0.01)
    const big = optimalNumBytes(10000, 0.01)
    expect(small).toBeGreaterThanOrEqual(32)
    expect(big).toBeGreaterThan(small)
    // Below 1024 should be a power of two
    if (small < 1024) expect(small & small - 1).toBe(0)
  })

  it('always returns a multiple of 32', () => {
    for (const ndv of [1, 10, 100, 1000, 10000, 100000]) {
      expect(optimalNumBytes(ndv, 0.01) % 32).toBe(0)
    }
  })

  it('looser FPP yields a smaller (or equal) filter', () => {
    const tight = optimalNumBytes(10000, 0.001)
    const loose = optimalNumBytes(10000, 0.1)
    expect(loose).toBeLessThanOrEqual(tight)
  })

  it('rejects fpp out of range', () => {
    expect(() => optimalNumBytes(100, 0)).toThrow()
    expect(() => optimalNumBytes(100, 1)).toThrow()
  })
})

describe('createBloomFilter', () => {
  it('returns a zeroed Uint32Array sized by optimalNumBytes', () => {
    const bf = createBloomFilter(1000, 0.01)
    expect(bf).toBeInstanceOf(Uint32Array)
    expect(bf.byteLength).toBe(optimalNumBytes(1000, 0.01))
    expect(bf.every(w => w === 0)).toBe(true)
  })

  it('defaults fpp to 0.01', () => {
    expect(createBloomFilter(1000).byteLength).toBe(optimalNumBytes(1000, 0.01))
  })
})

describe('sbbfInsert / sbbfContains', () => {
  it('contains every inserted hash (no false negatives)', () => {
    const bf = createBloomFilter(1000, 0.01)
    /** @type {bigint[]} */
    const hashes = []
    for (let i = 0; i < 500; i++) {
      const h = xxhash64(textEncoder.encode(`in-${i}`))
      hashes.push(h)
      sbbfInsert(bf, h)
    }
    for (const h of hashes) {
      expect(sbbfContains(bf, h)).toBe(true)
    }
  })

  it('false-positive rate is roughly bounded by the target FPP', () => {
    const ndv = 1000
    const fpp = 0.01
    const bf = createBloomFilter(ndv, fpp)
    for (let i = 0; i < ndv; i++) {
      sbbfInsert(bf, xxhash64(textEncoder.encode(`in-${i}`)))
    }
    let fp = 0
    const probes = 10000
    for (let i = 0; i < probes; i++) {
      if (sbbfContains(bf, xxhash64(textEncoder.encode(`probe-${i}`)))) fp++
    }
    expect(fp / probes).toBeLessThan(fpp * 3)
  })

  it('an empty filter contains no hash', () => {
    const bf = createBloomFilter(100, 0.01)
    expect(sbbfContains(bf, 0n)).toBe(false)
    expect(sbbfContains(bf, 0xdeadbeefdeadbeefn)).toBe(false)
  })
})

describe('writeBloomFilter', () => {
  it('round-trips through hyparquet readBloomFilter', () => {
    const bf = createBloomFilter(1000, 0.01)
    /** @type {bigint[]} */
    const hashes = []
    for (let i = 0; i < 200; i++) {
      const h = xxhash64(textEncoder.encode(`v-${i}`))
      hashes.push(h)
      sbbfInsert(bf, h)
    }
    const writer = new ByteWriter()
    writeBloomFilter(writer, bf)

    const bytes = writer.getBytes()
    const reader = { view: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength), offset: 0 }
    const parsed = readBloomFilter(reader)

    expect(parsed).toBeDefined()
    expect(parsed?.numBytes).toBe(bf.byteLength)
    expect(reader.offset).toBe(bytes.byteLength)
    expect(parsed?.blocks).toEqual(bf)
    for (const h of hashes) {
      expect(sbbfContains(parsed?.blocks ?? new Uint32Array(0), h)).toBe(true)
    }
  })

  it('rejects block arrays that are not a multiple of 8 words', () => {
    const writer = new ByteWriter()
    expect(() => writeBloomFilter(writer, new Uint32Array(7))).toThrow()
  })
})

describe('BloomBuilder', () => {
  it('collects values and finalizes to a filter containing each', () => {
    const builder = new BloomBuilder({ name: 's', type: 'BYTE_ARRAY' })
    const values = ['alpha', 'beta', 'gamma', 'beta', 'delta']
    for (const v of values) builder.insert(v)
    const blocks = builder.finalize()
    expect(blocks).toBeDefined()
    for (const v of values) {
      const h = hashParquetValue(v, { name: 's', type: 'BYTE_ARRAY' }) ?? 0n
      expect(sbbfContains(blocks ?? new Uint32Array(0), h)).toBe(true)
    }
  })

  it('skips null and undefined without disabling the filter', () => {
    const builder = new BloomBuilder({ name: 'i', type: 'INT32' })
    builder.insert(null)
    builder.insert(undefined)
    builder.insert(1)
    builder.insert(2)
    const blocks = builder.finalize()
    expect(blocks).toBeDefined()
  })

  it('sizes the filter for the distinct count, not the insert count', () => {
    const builder = new BloomBuilder({ name: 'i', type: 'INT32' }, { fpp: 0.01 })
    for (let i = 0; i < 10000; i++) builder.insert(i % 50) // 50 distinct
    const blocks = builder.finalize()
    expect(blocks?.byteLength).toBe(optimalNumBytes(50, 0.01))
  })

  it('returns undefined when no values were inserted', () => {
    const builder = new BloomBuilder({ name: 's', type: 'BYTE_ARRAY' })
    expect(builder.finalize()).toBeUndefined()
  })

  it('returns undefined when any non-null value is unhashable (would be a false negative)', () => {
    // INT32 column with a non-integer JS value — hashParquetValue returns undefined
    const builder = new BloomBuilder({ name: 'i', type: 'INT32' })
    builder.insert(1)
    builder.insert(1.5) // unhashable for INT32
    expect(builder.finalize()).toBeUndefined()
  })

  it('returns undefined when sizing exceeds maxBytes', () => {
    const builder = new BloomBuilder(
      { name: 's', type: 'BYTE_ARRAY' },
      { fpp: 0.001, maxBytes: 256 }
    )
    for (let i = 0; i < 1000; i++) builder.insert(`v-${i}`)
    expect(builder.finalize()).toBeUndefined()
  })
})

describe('hashParquetValue', () => {
  it('hashes BOOLEAN as a single byte', () => {
    expect(hashParquetValue(true, { name: 'b', type: 'BOOLEAN' })).toBeTypeOf('bigint')
    expect(hashParquetValue(false, { name: 'b', type: 'BOOLEAN' }))
      .not.toBe(hashParquetValue(true, { name: 'b', type: 'BOOLEAN' }))
  })

  it('hashes INT32 as a little-endian 4-byte value', () => {
    const h = hashParquetValue(42, { name: 'i', type: 'INT32' })
    expect(h).toBeTypeOf('bigint')
  })

  it('hashes INT64 from bigint and from safe number identically', () => {
    const a = hashParquetValue(42n, { name: 'i', type: 'INT64' })
    const b = hashParquetValue(42, { name: 'i', type: 'INT64' })
    expect(a).toBe(b)
  })

  it('hashes BYTE_ARRAY strings via UTF-8', () => {
    const h = hashParquetValue('abc', { name: 's', type: 'BYTE_ARRAY' })
    expect(h).toBe(0x44bc2cf5ad770999n)
  })

  it('returns undefined for null/undefined', () => {
    expect(hashParquetValue(null, { name: 's', type: 'BYTE_ARRAY' })).toBeUndefined()
    expect(hashParquetValue(undefined, { name: 's', type: 'BYTE_ARRAY' })).toBeUndefined()
  })

  it('returns undefined for type mismatches', () => {
    expect(hashParquetValue('not-a-number', { name: 'i', type: 'INT32' })).toBeUndefined()
    expect(hashParquetValue(1.5, { name: 'i', type: 'INT32' })).toBeUndefined()
    expect(hashParquetValue(123, { name: 's', type: 'BYTE_ARRAY' })).toBeUndefined()
  })

  it('returns undefined for lossy logical types (DATE, TIMESTAMP, DECIMAL, UUID, ...)', () => {
    expect(hashParquetValue(1, { name: 'd', type: 'INT32', converted_type: 'DATE' })).toBeUndefined()
    expect(hashParquetValue(1n, { name: 't', type: 'INT64', converted_type: 'TIMESTAMP_MILLIS' })).toBeUndefined()
    expect(hashParquetValue('{}', { name: 'j', type: 'BYTE_ARRAY', converted_type: 'JSON' })).toBeUndefined()
    expect(hashParquetValue(new Uint8Array(16), { name: 'u', type: 'FIXED_LEN_BYTE_ARRAY', logical_type: { type: 'UUID' } })).toBeUndefined()
  })
})

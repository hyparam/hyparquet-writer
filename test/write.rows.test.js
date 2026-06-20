import { parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { ByteWriter, parquetWriteBuffer, parquetWriteRows } from '../src/index.js'

/**
 * @param {any} args
 * @returns {ArrayBuffer}
 */
function writeRows(args) {
  const writer = new ByteWriter()
  parquetWriteRows({ writer, ...args })
  return writer.getBuffer()
}

describe('parquetWriteRows', () => {
  /** @type {Record<string, any>[]} */
  const rows = Array.from({ length: 300 }, (_, i) => ({
    id: i,
    score: i * 0.5,
    name: `name-${i % 13}`,
    flag: (i & 1) === 0,
    label: i % 5 === 0 ? null : `label-${i % 7}`,
  }))
  const columns = ['id', 'score', 'name', 'flag', 'label'].map(name => ({ name }))

  it('produces byte-identical output to the equivalent transposed columns', () => {
    const names = ['id', 'score', 'name', 'flag', 'label']
    const columnData = names.map(name => ({ name, data: rows.map(r => r[name]) }))
    const fromColumns = parquetWriteBuffer({ columnData, rowGroupSize: 64 })
    const fromRows = writeRows({ rows, columns, rowGroupSize: 64 })
    expect(new Uint8Array(fromRows)).toEqual(new Uint8Array(fromColumns))
  })

  it('round-trips with windows spanning row groups', async () => {
    const buffer = writeRows({ rows, columns, rowGroupSize: 50 })
    const out = await parquetReadObjects({ file: buffer })
    expect(out).toEqual(rows)
  })

  it('respects an explicit column list (order, type, subset)', async () => {
    const buffer = writeRows({
      rows,
      columns: [
        { name: 'name', type: 'STRING' },
        { name: 'id', type: 'INT32' },
      ],
      rowGroupSize: 64,
    })
    const out = await parquetReadObjects({ file: buffer })
    expect(out[0]).toEqual({ name: 'name-0', id: 0 })
    expect(out.length).toBe(rows.length)
  })

  it('throws when columns is missing or empty', () => {
    expect(() => writeRows({ rows: [{ a: 1, b: 'x' }] })).toThrow('parquetWriteRows requires a non-empty columns array')
    expect(() => writeRows({ rows: [{ a: 1, b: 'x' }], columns: [] })).toThrow('parquetWriteRows requires a non-empty columns array')
  })

  it('handles missing fields as nulls', async () => {
    const sparse = [{ a: 1, b: 2 }, { a: 3 }, { b: 4 }]
    const buffer = writeRows({
      rows: sparse,
      columns: [{ name: 'a', type: 'INT32' }, { name: 'b', type: 'INT32' }],
    })
    const out = await parquetReadObjects({ file: buffer })
    expect(out).toEqual([{ a: 1, b: 2 }, { a: 3, b: null }, { a: null, b: 4 }])
  })

  it('throws when rowGroupSize is an empty array', () => {
    expect(() => writeRows({ rows, columns, rowGroupSize: [] })).toThrow('rowGroupSize array cannot be empty')
  })

  it('throws when rows is neither array nor iterable', () => {
    expect(() => writeRows({ rows: null, columns: [{ name: 'a' }] })).toThrow('parquetWriteRows expects a rows array, iterable, or async iterable')
    expect(() => writeRows({ rows: 42, columns: [{ name: 'a' }] })).toThrow('parquetWriteRows expects a rows array, iterable, or async iterable')
  })

  describe('streaming iterable input', () => {
    it('produces byte-identical output to the array path', () => {
      const fromArray = writeRows({ rows, columns })
      const fromGen = writeRows({ rows: (function* () { yield* rows })(), columns })
      expect(new Uint8Array(fromGen)).toEqual(new Uint8Array(fromArray))
    })

    it('round-trips a generator across many row groups', async () => {
      function* gen() {
        for (let i = 0; i < 2500; i++) yield { a: i, b: `v${i % 9}` }
      }
      const buffer = writeRows({ rows: gen(), columns: [{ name: 'a' }, { name: 'b' }], rowGroupSize: 500 })
      const out = await parquetReadObjects({ file: buffer })
      expect(out.length).toBe(2500)
      expect(out[0]).toEqual({ a: 0, b: 'v0' })
      expect(out[2499]).toEqual({ a: 2499, b: 'v6' })
    })

    it('infers schema from the first buffered group', async () => {
      const buffer = writeRows({
        rows: (function* () { yield* rows })(),
        columns,
        rowGroupSize: 100,
      })
      const out = await parquetReadObjects({ file: buffer })
      expect(out).toEqual(rows)
    })

    it('respects an explicit column list for a generator', async () => {
      function* gen() {
        for (let i = 0; i < 50; i++) yield { x: i, y: i * 2, z: 'skip' }
      }
      const buffer = writeRows({
        rows: gen(),
        columns: [{ name: 'x', type: 'INT32' }, { name: 'y', type: 'INT32' }],
        rowGroupSize: 16,
      })
      const out = await parquetReadObjects({ file: buffer })
      expect(out.length).toBe(50)
      expect(out[0]).toEqual({ x: 0, y: 0 })
    })

    it('handles an empty iterable', async () => {
      const buffer = writeRows({ rows: (function* () {})(), columns: [{ name: 'a', type: 'INT32' }] })
      const out = await parquetReadObjects({ file: buffer })
      expect(out).toEqual([])
    })
  })

  /**
   * A ByteWriter with an async flush, to drive the async sink path.
   * @param {() => Promise<void>} [onFlush]
   * @returns {any}
   */
  function asyncWriter(onFlush) {
    /** @type {any} */
    const writer = new ByteWriter()
    writer.flush = onFlush ?? (() => Promise.resolve())
    return writer
  }

  describe('async sink', () => {
    it('returns a promise and round-trips with an async flush', async () => {
      const writer = asyncWriter()
      const result = parquetWriteRows({ writer, rows, columns, rowGroupSize: 50 })
      expect(result).toBeInstanceOf(Promise)
      await result
      const out = await parquetReadObjects({ file: writer.getBuffer() })
      expect(out).toEqual(rows)
    })

    it('applies backpressure to a lazy source, pulling one group ahead', async () => {
      let pulled = 0
      function* gen() {
        for (let i = 0; i < 250; i++) {
          pulled++
          yield { a: i, b: `v${i}` }
        }
      }
      /** @type {number[]} */
      const pulledAtFlush = []
      const writer = asyncWriter(async () => {
        await Promise.resolve()
        pulledAtFlush.push(pulled)
      })
      await parquetWriteRows({
        writer,
        rows: gen(),
        columns: [{ name: 'a' }, { name: 'b' }],
        rowGroupSize: 50,
      })
      // Each group's write completes before the next is pulled from the source,
      // so the source never runs ahead of the writer by more than one group.
      expect(pulledAtFlush).toEqual([50, 100, 150, 200, 250])
      const out = await parquetReadObjects({ file: writer.getBuffer() })
      expect(out.length).toBe(250)
    })

    it('stays synchronous for a sync sink', () => {
      const writer = new ByteWriter()
      const result = parquetWriteRows({ writer, rows, columns, rowGroupSize: 50 })
      expect(result).toBeUndefined()
    })
  })

  describe('async iterable input', () => {
    it('returns a promise and round-trips an async generator across row groups', async () => {
      async function* gen() {
        for (const row of rows) {
          await Promise.resolve()
          yield row
        }
      }
      const writer = new ByteWriter()
      const result = parquetWriteRows({ writer, rows: gen(), columns, rowGroupSize: 50 })
      expect(result).toBeInstanceOf(Promise)
      await result
      const out = await parquetReadObjects({ file: writer.getBuffer() })
      expect(out).toEqual(rows)
    })

    it('produces output identical to the equivalent array input', async () => {
      async function* gen() {
        yield* rows
      }
      const writer = new ByteWriter()
      await parquetWriteRows({ writer, rows: gen(), columns, rowGroupSize: 64 })
      const fromArray = writeRows({ rows, columns, rowGroupSize: 64 })
      expect(new Uint8Array(writer.getBuffer())).toEqual(new Uint8Array(fromArray))
    })

    it('infers schema from the first buffered group', async () => {
      async function* gen() {
        yield* rows
      }
      const writer = new ByteWriter()
      await parquetWriteRows({ writer, rows: gen(), columns, rowGroupSize: 50 })
      const out = await parquetReadObjects({ file: writer.getBuffer() })
      expect(out).toEqual(rows)
    })

    it('handles an empty async iterable', async () => {
      async function* gen() {}
      const writer = new ByteWriter()
      await parquetWriteRows({ writer, rows: gen(), columns: [{ name: 'a', type: 'INT32' }] })
      const out = await parquetReadObjects({ file: writer.getBuffer() })
      expect(out).toEqual([])
    })

    it('does not pull a group ahead of the writer', async () => {
      let pulled = 0
      async function* gen() {
        for (let i = 0; i < 250; i++) {
          await Promise.resolve()
          pulled++
          yield { a: i, b: `v${i}` }
        }
      }
      // Record how many rows had been pulled from the source at each flush.
      /** @type {number[]} */
      const pulledAtFlush = []
      /** @type {any} */
      const writer = new ByteWriter()
      writer.flush = async () => {
        await Promise.resolve()
        pulledAtFlush.push(pulled)
      }
      await parquetWriteRows({
        writer,
        rows: gen(),
        columns: [{ name: 'a' }, { name: 'b' }],
        rowGroupSize: 50,
      })
      // The next group is pulled only after the prior group's write settles, so
      // the source is never read more than the current group ahead of the writer.
      expect(pulledAtFlush).toEqual([50, 100, 150, 200, 250])
      const out = await parquetReadObjects({ file: writer.getBuffer() })
      expect(out.length).toBe(250)
    })
  })
})

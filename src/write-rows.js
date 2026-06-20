import { ParquetWriter, groupSize } from './parquet-writer.js'
import { schemaFromColumnData } from './schema.js'

/**
 * @import {DecodedArray} from 'hyparquet'
 * @import {ColumnSource, ParquetWriteRowsOptions} from '../src/types.js'
 */

/**
 * Write row objects to parquet without first transposing the whole dataset into
 * columns, so peak memory is bounded by the row-group size, not the dataset.
 *
 * `rows` may be an array, any sync iterable (a generator, a Set), or an async
 * iterable (a DB cursor, a stream). With a lazy source the rows are pulled one
 * group at a time and never all held at once, so peak memory is independent of
 * the total row count.
 *
 * Return type: an async-iterable source always returns a promise, since its
 * rows can't be pulled synchronously. For an array or sync iterable it is
 * governed by the sink — an async `writer` (its `flush` returns a promise)
 * returns a promise, a fully synchronous sink returns void. Either way each
 * group's write settles before the next group is pulled, so the source and sink
 * stay within one group of each other (see drain / drainAsync below).
 *
 * `columns` is required and fixes the column names and order (same fields as
 * ColumnSource minus the data); per-column `type` is optional. A schema is
 * inferred from the first group's values unless one is supplied.
 *
 * Takes the same write options as {@link parquetWrite} (codec, compressors,
 * statistics, rowGroupSize, pageSize, kvMetadata, schema) at the top level,
 * minus `columnData`, since `rows` and `columns` describe the data instead.
 *
 * @param {ParquetWriteRowsOptions} options
 * @returns {void | Promise<void>}
 */
export function parquetWriteRows({ writer, rows, columns, schema, rowGroupSize = [1000, 100000], pageSize, ...options }) {
  if (!Array.isArray(columns) || columns.length === 0) {
    throw new Error('parquetWriteRows requires a non-empty columns array')
  }
  const isArray = Array.isArray(rows)
  // Iterating the union type needs no casts if we go through one `any` alias;
  // the array fast path keeps using the typed `rows` for indexed access.
  /** @type {any} */
  const source = rows
  // An async iterable (DB cursor, stream) takes precedence: a value may expose
  // both iterators, but if it has an async one we must pull with `for await`.
  const isAsync = !isArray && source && typeof source[Symbol.asyncIterator] === 'function'
  const isSync = !isArray && source && typeof source[Symbol.iterator] === 'function'
  if (!isArray && !isAsync && !isSync) {
    throw new Error('parquetWriteRows expects a rows array, iterable, or async iterable')
  }
  if (Array.isArray(rowGroupSize) && !rowGroupSize.length) {
    throw new Error('rowGroupSize array cannot be empty')
  }
  const fields = columns.map(s => s.name)

  /** @type {ParquetWriter | undefined} */
  let pq

  /**
   * Yield successive row-group windows. For an array these index straight into
   * `rows` with no buffering; for a lazy iterable each window is a freshly
   * buffered batch, pulled from the source only when the consumer asks for the
   * next window, so under backpressure the source is never read ahead of the
   * writer.
   * @yields {{ src: Record<string, any>[], start: number, size: number }}
   */
  function* windows() {
    if (isArray) {
      let i = 0
      let g = 0
      while (i < rows.length) {
        const size = Math.min(groupSize(rowGroupSize, g++), rows.length - i)
        yield { src: rows, start: i, size }
        i += size
      }
    } else {
      /** @type {Record<string, any>[]} */
      let batch = []
      let g = 0
      let target = groupSize(rowGroupSize, 0)
      // This branch only runs for a sync iterable; the async case uses drainAsync.
      for (const row of source) {
        batch.push(row)
        if (batch.length >= target) {
          yield { src: batch, start: 0, size: batch.length }
          batch = []
          target = groupSize(rowGroupSize, ++g)
        }
      }
      if (batch.length) yield { src: batch, start: 0, size: batch.length }
    }
  }

  /**
   * Transpose one window of rows and write it as a single row group, lazily
   * creating the writer (and inferring the schema from the first group) on the
   * first call. Returns the writer's promise iff the sink flushed asynchronously.
   * @param {Record<string, any>[]} src
   * @param {number} start
   * @param {number} size
   * @returns {void | Promise<void>}
   */
  function writeWindow(src, start, size) {
    // Transpose this group's rows into one array per column, then hand the core
    // columnar path its usual { ...spec, data } shape: this is a thin wrapper
    // that feeds groups in incrementally, not a new column-input type.
    const cols = transposeWindow(src, fields, start, size)
    /** @type {ColumnSource[]} */
    const columnData = columns.map((spec, c) => ({ ...spec, data: cols[c] }))
    // The first group fixes the schema, so the writer can't be created earlier.
    if (!pq) {
      pq = new ParquetWriter({ writer, schema: schema ?? schemaFromColumnData({ columnData }), ...options })
    }
    return pq.write({ columnData, rowGroupSize: size, pageSize })
  }

  const it = windows()

  /**
   * Pull, transpose, and write windows in order. Returns a promise iff a write
   * was async, in which case it resumes the loop only after that write settles
   * so the source can't run ahead of the writer.
   * @returns {void | Promise<void>}
   */
  function drain() {
    for (let next = it.next(); !next.done; next = it.next()) {
      const { src, start, size } = next.value
      const r = writeWindow(src, start, size)
      if (r) return r.then(drain)
    }
  }

  /**
   * Async-source variant of {@link drain}: pull rows from an async iterable one
   * group at a time, awaiting each group's write before pulling the next so the
   * source (a cursor or stream) is never read ahead of the writer. Only ever
   * one buffered batch plus its columnar copy is held, so peak memory stays
   * bounded by the row-group size regardless of total row count.
   * @returns {Promise<void>}
   */
  async function drainAsync() {
    /** @type {Record<string, any>[]} */
    let batch = []
    let g = 0
    let target = groupSize(rowGroupSize, 0)
    for await (const row of source) {
      batch.push(row)
      if (batch.length >= target) {
        await writeWindow(batch, 0, batch.length)
        batch = []
        target = groupSize(rowGroupSize, ++g)
      }
    }
    if (batch.length) await writeWindow(batch, 0, batch.length)
  }

  /**
   * Emit an empty file if no rows were written, then finish.
   * @returns {void | Promise<void>}
   */
  function finish() {
    if (!pq) {
      // No rows written: emit a valid empty file with the declared columns.
      /** @type {ColumnSource[]} */
      const columnData = columns.map(spec => ({ ...spec, data: [] }))
      pq = new ParquetWriter({ writer, schema: schema ?? schemaFromColumnData({ columnData }), ...options })
      const w = pq.write({ columnData, rowGroupSize, pageSize })
      if (w) return w.then(() => pq?.finish())
    }
    return pq?.finish()
  }

  // An async source forces an async return: the rows can't be pulled synchronously.
  if (isAsync) return drainAsync().then(finish)
  const drained = drain()
  return drained ? drained.then(finish) : finish()
}

/**
 * Transpose a window of row objects into one array per field, in a single pass.
 *
 * @param {Record<string, any>[]} rows
 * @param {string[]} fields
 * @param {number} start
 * @param {number} size
 * @returns {DecodedArray[]}
 */
function transposeWindow(rows, fields, start, size) {
  const width = fields.length
  const cols = new Array(width)
  for (let c = 0; c < width; c++) cols[c] = new Array(size)
  for (let k = 0; k < size; k++) {
    const row = rows[start + k]
    for (let c = 0; c < width; c++) cols[c][k] = row[fields[c]]
  }
  return cols
}

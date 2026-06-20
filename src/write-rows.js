import { ParquetWriter, groupSize } from './parquet-writer.js'
import { schemaFromColumnData } from './schema.js'

/**
 * @import {DecodedArray} from 'hyparquet'
 * @import {ColumnSource, ParquetWriteRowsOptions} from '../src/types.js'
 */

/**
 * Write row objects to parquet without first transposing the whole dataset into
 * columns. Rows are processed one row group at a time: transpose that group,
 * write it through an incrementally-driven ParquetWriter, then discard it. Only
 * one group is ever materialized in column-major form, so peak memory is bounded
 * by the row-group size, not the dataset.
 *
 * `rows` may be an array or any (sync) iterable, such as a generator, a Set, or
 * a DB cursor. With a lazy iterable the rows are pulled one group at a time and
 * never all held at once, so peak memory is independent of the total row count.
 *
 * If the `writer` sink is async (its `flush` returns a promise), the write runs
 * with backpressure: each group's write is awaited before the next group is
 * transposed or pulled from the source, and the call returns a promise. With a
 * fully synchronous sink no promise is ever created and the call returns void.
 *
 * This is a thin wrapper over ParquetWriter and the plain columnar write path;
 * it adds no new column-input shapes to the core writer.
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
  if (!isArray && !(rows && typeof rows[Symbol.iterator] === 'function')) {
    throw new Error('parquetWriteRows expects a rows array or iterable')
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
      for (const row of rows) {
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

  const it = windows()

  /**
   * Pull, transpose, and write windows in order, lazily creating the writer
   * (and inferring the schema from the first group) on the first call.
   * promise iff a write was async.
   * @returns {void | Promise<void>}
   */
  function drain() {
    for (let next = it.next(); !next.done; next = it.next()) {
      // Transpose this group's rows into one array per column.
      const { src, start, size } = next.value
      const cols = transposeWindow(src, fields, start, size)
      /** @type {ColumnSource[]} */
      const columnData = columns.map((spec, c) => ({ ...spec, data: cols[c] }))
      // The first group fixes the schema, so the writer can't be created earlier.
      if (!pq) {
        pq = new ParquetWriter({ writer, schema: schema ?? schemaFromColumnData({ columnData }), ...options })
      }
      // Async write: await it before the next it.next() so the source can't run ahead.
      const r = pq.write({ columnData, rowGroupSize: size, pageSize })
      if (r) return r.then(drain)
    }
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

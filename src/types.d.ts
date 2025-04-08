import type { DecodedArray, ParquetType } from 'hyparquet'
import type { KeyValue } from 'hyparquet/src/types.js' // TODO export from hyparquet

export interface ParquetWriteOptions {
  writer: Writer
  columnData: ColumnData[]
  compressed?: boolean
  statistics?: boolean
  rowGroupSize?: number
  kvMetadata?: KeyValue[]
}

export interface ColumnData {
  name: string
  data: DecodedArray
  type?: ParquetType
}

export interface Writer {
  buffer: ArrayBuffer
  offset: number
  view: DataView
  ensure(size: number): void
  finish(): void
  getBuffer(): ArrayBuffer
  appendUint8(value: number): void
  appendUint32(value: number): void
  appendInt32(value: number): void
  appendInt64(value: bigint): void
  appendFloat64(value: number): void
  appendBuffer(buffer: ArrayBuffer): void
  appendBytes(value: Uint8Array): void
  appendVarInt(value: number): void
  appendVarBigInt(value: bigint): void
}

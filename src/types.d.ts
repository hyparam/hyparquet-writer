import type { DecodedArray, KeyValue, SchemaElement } from 'hyparquet'

// Superset of parquet types with automatic conversions
export type BasicType =
  'BOOLEAN' |
  'INT32' |
  'INT64' |
  'FLOAT' |
  'DOUBLE' |
  'BYTE_ARRAY' |
  'STRING' |
  'JSON' |
  'TIMESTAMP' |
  'UUID' |
  'FLOAT16' |
  'GEOMETRY' |
  'GEOGRAPHY'

export interface ParquetWriteOptions {
  writer: Writer
  columnData: ColumnSource[]
  schema?: SchemaElement[]
  compressed?: boolean
  statistics?: boolean
  rowGroupSize?: number | number[]
  kvMetadata?: KeyValue[]
}

export interface ColumnSource {
  name: string
  data: DecodedArray
  type?: BasicType
  nullable?: boolean
}

export interface ListValues {
  values: any[]
  definitionLevels: number[]
  repetitionLevels: number[]
  numNulls: number
}

export interface ColumnEncoder {
  columnName: string
  element: SchemaElement
  schemaPath: SchemaElement[]
  compressed: boolean
}

export interface Writer {
  buffer: ArrayBuffer
  view: DataView
  offset: number

  ensure(size: number): void
  finish(): void
  getBuffer(): ArrayBuffer
  appendUint8(value: number): void
  appendUint32(value: number): void
  appendInt32(value: number): void
  appendInt64(value: bigint): void
  appendFloat32(value: number): void
  appendFloat64(value: number): void
  appendBuffer(buffer: ArrayBuffer): void
  appendBytes(value: Uint8Array): void
  appendVarInt(value: number): void
  appendVarBigInt(value: bigint): void
}

export type ThriftObject = { [ key: `field_${number}` ]: ThriftType }
export type ThriftType = boolean | number | bigint | string | Uint8Array | ThriftType[] | ThriftObject | undefined

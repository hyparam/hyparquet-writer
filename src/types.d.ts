import type { ColumnChunk, ColumnIndex, CompressionCodec, DecodedArray, Encoding, KeyValue, OffsetIndex, SchemaElement } from 'hyparquet'

export type Compressor = (input: Uint8Array) => Uint8Array
export type Compressors = { [K in CompressionCodec]?: Compressor }

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
  codec?: CompressionCodec // global default codec, default 'SNAPPY'
  compressors?: Compressors // custom compressors
  statistics?: boolean // enable column statistics, default true
  rowGroupSize?: number | number[] // number of rows per row group
  pageSize?: number // target uncompressed page size in bytes, default 1048576
  kvMetadata?: KeyValue[]
}

export interface ColumnSource {
  name: string
  data: DecodedArray
  type?: BasicType
  nullable?: boolean
  encoding?: Encoding
  columnIndex?: boolean // write column indexes, default false
  offsetIndex?: boolean // write offset indexes, default true
}

export interface PageData {
  values: DecodedArray
  definitionLevels: number[]
  repetitionLevels: number[]
  numNulls: number
  maxDefinitionLevel: number
}

export interface ColumnEncoder {
  columnName: string
  element: SchemaElement
  schemaPath: SchemaElement[]
  codec: CompressionCodec
  compressors: Compressors
  stats: boolean
  pageSize: number
  // Spec: If ColumnIndex is present, OffsetIndex must also be present
  columnIndex: boolean
  offsetIndex: boolean
  encoding?: Encoding // user-specified encoding
}

export interface PageIndexes {
  chunk: ColumnChunk
  columnIndex?: ColumnIndex
  offsetIndex?: OffsetIndex
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
  appendZigZag(value: number | bigint): void
}

export type ThriftObject = { [ key: `field_${number}` ]: ThriftType }
export type ThriftType = boolean | number | bigint | string | Uint8Array | ThriftType[] | ThriftObject | undefined

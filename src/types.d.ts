
export interface Writer {
  buffer: ArrayBuffer
  offset: number
  appendUint8(value: number): void
  appendUint32(value: number): void
  appendFloat64(value: number): void
  appendBuffer(buffer: ArrayBuffer): void
  appendVarInt(value: number): void
  appendVarBigInt(value: bigint): void
}

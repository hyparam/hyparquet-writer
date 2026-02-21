import { ByteWriter } from './bytewriter.js'

/**
 * @import {Geometry, Position} from 'hyparquet/src/types.js'
 */

/**
 * Serialize a GeoJSON geometry into ISO WKB.
 *
 * @param {Geometry} geometry
 * @returns {Uint8Array}
 */
export function geojsonToWkb(geometry) {
  const writer = new ByteWriter()
  writeGeometry(writer, geometry)
  return new Uint8Array(writer.getBuffer())
}

/**
 * @param {ByteWriter} writer
 * @param {Geometry} geometry
 */
function writeGeometry(writer, geometry) {
  if (typeof geometry !== 'object') {
    throw new Error('geometry values must be GeoJSON geometries')
  }
  const typeCode = geometryTypeCode(geometry.type)

  // infer dimensions
  const dim = inferGeometryDimensions(geometry)
  let flag = 0
  if (dim === 3) flag = 1
  else if (dim === 4) flag = 3
  else if (dim > 4) throw new Error(`unsupported geometry dimensions: ${dim}`)

  writer.appendUint8(1) // little endian
  writer.appendUint32(typeCode + flag * 1000)

  if (geometry.type === 'Point') {
    writePosition(writer, geometry.coordinates, dim)
  } else if (geometry.type === 'LineString') {
    writeLine(writer, geometry.coordinates, dim)
  } else if (geometry.type === 'Polygon') {
    writer.appendUint32(geometry.coordinates.length)
    for (const ring of geometry.coordinates) {
      writeLine(writer, ring, dim)
    }
  } else if (geometry.type === 'MultiPoint') {
    writer.appendUint32(geometry.coordinates.length)
    for (const coordinates of geometry.coordinates) {
      writeGeometry(writer, { type: 'Point', coordinates })
    }
  } else if (geometry.type === 'MultiLineString') {
    writer.appendUint32(geometry.coordinates.length)
    for (const coordinates of geometry.coordinates) {
      writeGeometry(writer, { type: 'LineString', coordinates })
    }
  } else if (geometry.type === 'MultiPolygon') {
    writer.appendUint32(geometry.coordinates.length)
    for (const coordinates of geometry.coordinates) {
      writeGeometry(writer, { type: 'Polygon', coordinates })
    }
  } else if (geometry.type === 'GeometryCollection') {
    writer.appendUint32(geometry.geometries.length)
    for (const child of geometry.geometries) {
      writeGeometry(writer, child)
    }
  } else {
    throw new Error('unsupported geometry type')
  }
}

/**
 * @param {ByteWriter} writer
 * @param {Position} position
 * @param {number} dim
 */
function writePosition(writer, position, dim) {
  if (position.length < dim) {
    throw new Error('geometry position dimensions mismatch')
  }
  for (let i = 0; i < dim; i++) {
    writer.appendFloat64(position[i])
  }
}

/**
 * @param {ByteWriter} writer
 * @param {Position[]} coordinates
 * @param {number} dim
 */
function writeLine(writer, coordinates, dim) {
  writer.appendUint32(coordinates.length)
  for (const position of coordinates) {
    writePosition(writer, position, dim)
  }
}

/**
 * @param {Geometry['type']} type
 * @returns {number}
 */
function geometryTypeCode(type) {
  if (type === 'Point') return 1
  if (type === 'LineString') return 2
  if (type === 'Polygon') return 3
  if (type === 'MultiPoint') return 4
  if (type === 'MultiLineString') return 5
  if (type === 'MultiPolygon') return 6
  if (type === 'GeometryCollection') return 7
  throw new Error(`unknown geometry type: ${type}`)
}

/**
 * Determine the maximum coordinate dimensions for the geometry.
 *
 * @param {Geometry} geometry
 * @returns {number}
 */
function inferGeometryDimensions(geometry) {
  if (geometry.type === 'GeometryCollection') {
    let maxDim = 0
    for (const child of geometry.geometries) {
      maxDim = Math.max(maxDim, inferGeometryDimensions(child))
    }
    return maxDim || 2
  }
  return inferCoordinateDimensions(geometry.coordinates)
}

/**
 * @param {any} value
 * @returns {number}
 */
function inferCoordinateDimensions(value) {
  if (!Array.isArray(value)) return 2
  if (!value.length) return 2
  if (typeof value[0] === 'number') return value.length
  let maxDim = 0
  for (const item of value) {
    maxDim = Math.max(maxDim, inferCoordinateDimensions(item))
  }
  return maxDim || 2
}

import { getMaxDefinitionLevel } from './schema.js'

/**
 * @import {DecodedArray, SchemaElement, SchemaTree} from 'hyparquet'
 * @import {PageData} from '../src/types.js'
 */

/**
 * Encode column values into repetition and definition levels
 * following the Dremel algorithm. For nested columns, this extracts
 * leaf values from the nested structure.
 *
 * @param {SchemaElement[]} schemaPath schema elements from root to leaf
 * @param {DecodedArray} rows column data for the current row group
 * @returns {PageData}
 */
export function encodeNestedValues(schemaPath, rows) {
  if (schemaPath.length < 2) throw new Error('parquet schema path must include column')

  /** @type {number[]} */
  const definitionLevels = []
  /** @type {number[]} */
  const repetitionLevels = []
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)

  // Flat required columns don't need Dremel encoding
  if (schemaPath.length === 2 && maxDefinitionLevel === 0) {
    return { values: rows, definitionLevels, repetitionLevels, maxDefinitionLevel }
  }

  // Track repetition depth prior to each level
  const repLevelPrior = new Array(schemaPath.length)
  let repeatedCount = 0
  for (let i = 0; i < schemaPath.length; i++) {
    repLevelPrior[i] = repeatedCount
    if (schemaPath[i].repetition_type === 'REPEATED') repeatedCount++
  }

  /** @type {any[]} */
  const values = []

  for (const row of rows) {
    visit(1, row, 0, 0, false)
  }

  return { values, definitionLevels, repetitionLevels, maxDefinitionLevel }

  /**
   * Recursively walk the schema path, emitting definition/repetition pairs.
   *
   * @param {number} depth index into schemaPath
   * @param {any} value value at the current depth
   * @param {number} defLevel definition level accumulated so far
   * @param {number} repLevel repetition level for the next emitted slot
   * @param {boolean} allowNull whether the current value is allowed to be null
   */
  function visit(depth, value, defLevel, repLevel, allowNull) {
    const element = schemaPath[depth]
    const repetition = element.repetition_type || 'REQUIRED'

    // Leaf node
    if (depth === schemaPath.length - 1) {
      if (value === null || value === undefined) {
        if (repetition === 'REQUIRED' && !allowNull) {
          throw new Error('parquet required value is undefined')
        }
        definitionLevels.push(defLevel)
      } else {
        definitionLevels.push(repetition === 'REQUIRED' ? defLevel : defLevel + 1)
      }
      repetitionLevels.push(repLevel)
      values.push(value)
      return
    }

    if (repetition === 'REPEATED') {
      if (value === null || value === undefined) {
        if (!allowNull) throw new Error('parquet required value is undefined')
        visit(depth + 1, undefined, defLevel, repLevel, true)
        return
      }
      if (!Array.isArray(value)) {
        throw new Error(`parquet repeated field ${element.name} must be an array`)
      }
      if (!value.length) {
        visit(depth + 1, undefined, defLevel, repLevel, true)
        return
      }
      // For MAP key_value entries, extract the child field (key or value) from each entry
      const isMapEntry = schemaPath[depth - 1]?.converted_type === 'MAP'
      const childElement = schemaPath[depth + 1]
      for (let i = 0; i < value.length; i++) {
        let childValue = value[i]
        if (isMapEntry && childValue && typeof childValue === 'object' && childElement) {
          childValue = childValue[childElement.name]
        }
        const childRep = i === 0 ? repLevel : repLevelPrior[depth] + 1
        visit(depth + 1, childValue, defLevel + 1, childRep, false)
      }
      return
    }

    if (repetition === 'OPTIONAL') {
      if (value === null || value === undefined) {
        visit(depth + 1, undefined, defLevel, repLevel, true)
      } else {
        const childValue = getChildValue(depth, value)
        const childIsNull = childValue === null || childValue === undefined
        // Increment def level if: (1) this is a struct (contributes to def even if child is null),
        // or (2) the child value exists. LIST/MAP wrappers don't increment def level themselves.
        const isLogicalContainer = element?.converted_type === 'LIST' || element?.converted_type === 'MAP'
        const isStruct = element.num_children && !element.type && !isLogicalContainer
        const nextDef = isStruct || !childIsNull ? defLevel + 1 : defLevel
        visit(depth + 1, childIsNull ? undefined : childValue, nextDef, repLevel, childIsNull)
      }
      return
    }

    // REQUIRED
    if (value === null || value === undefined) {
      if (!allowNull) throw new Error('parquet required value is undefined')
      visit(depth + 1, undefined, defLevel, repLevel, true)
    } else {
      visit(depth + 1, getChildValue(depth, value), defLevel, repLevel, false)
    }
  }

  /**
   * Select the child value for the next schema element in the path.
   *
   * @param {number} depth current schema depth
   * @param {any} currentValue current value at this depth
   * @returns {any}
   */
  function getChildValue(depth, currentValue) {
    if (currentValue === null || currentValue === undefined) return undefined
    const child = schemaPath[depth + 1]
    if (!child) return undefined
    const parent = schemaPath[depth]

    // LIST and MAP wrappers don't correspond to user-visible properties
    if (parent?.converted_type === 'LIST' && child.name === 'list') return currentValue
    if (parent?.converted_type === 'MAP') return currentValue

    if (typeof currentValue === 'object') {
      return currentValue[child.name]
    }
    return undefined
  }

}

/**
 * Normalize a column value to the canonical form expected by the encoder:
 * - Structs become plain objects with normalized children
 * - Lists are ensured to be arrays whose elements are normalized recursively
 * - Maps are converted to arrays of { key, value } entries with normalized key/value
 *
 * @param {SchemaTree} node schema tree node for the column
 * @param {any} value
 * @returns {any}
 */
export function normalizeValue(node, value) {
  if (value === null || value === undefined) return value
  if (isListLikeNode(node)) {
    if (!Array.isArray(value)) throw new Error(`parquet list field ${node.element.name} must be an array`)
    const elementNode = node.children[0].children[0]
    return value.map(entry => normalizeValue(elementNode, entry))
  }
  if (isMapLikeNode(node)) {
    return normalizeMapEntries(node, value)
  }
  if (node.children.length) {
    if (typeof value !== 'object' || Array.isArray(value)) {
      throw new Error(`parquet struct field ${node.element.name} must be an object`)
    }
    /** @type {Record<string, any>} */
    const out = {}
    for (const child of node.children) {
      const childName = child.element.name
      const childValue = value[childName]
      if (child.element.repetition_type === 'REQUIRED' && (childValue === null || childValue === undefined)) {
        throw new Error('parquet required value is undefined')
      }
      out[childName] = normalizeValue(child, childValue)
    }
    return out
  }
  return value
}

/**
 * @param {SchemaTree} node
 * @param {any} value
 * @returns {{key: any, value: any}[]}
 */
function normalizeMapEntries(node, value) {
  if (value === null || value === undefined) return value
  /** @type {any[][]} */
  let entries
  if (value instanceof Map) {
    entries = Array.from(value.entries())
  } else if (Array.isArray(value)) {
    entries = value.map(entry => {
      if (entry && typeof entry === 'object' && 'key' in entry && 'value' in entry) {
        return [entry.key, entry.value]
      }
      if (Array.isArray(entry) && entry.length === 2) {
        return entry
      }
      throw new Error('parquet map entry must provide key and value')
    })
  } else if (typeof value === 'object') {
    entries = Object.entries(value)
  } else {
    throw new Error(`parquet map field ${node.element.name} must be Map, array, or object`)
  }
  const valueNode = node.children[0].children[1]
  return entries.map(([key, entryValue]) => ({
    key,
    value: normalizeValue(valueNode, entryValue),
  }))
}

/**
 * @param {SchemaTree} node
 * @returns {boolean}
 */
function isListLikeNode(node) {
  if (!node) return false
  if (node.element.converted_type !== 'LIST') return false
  if (node.children.length !== 1) return false
  const listNode = node.children[0]
  if (listNode.element.name !== 'list') return false
  if (listNode.children.length !== 1) return false
  if (listNode.element.repetition_type !== 'REPEATED') return false
  const elementNode = listNode.children[0]
  return elementNode.element.name === 'element'
}

/**
 * @param {SchemaTree} node
 * @returns {boolean}
 */
function isMapLikeNode(node) {
  if (!node) return false
  if (node.element.converted_type !== 'MAP') return false
  if (node.children.length !== 1) return false
  const entryNode = node.children[0]
  if (entryNode.children.length !== 2) return false
  if (entryNode.children[0].element.name !== 'key') return false
  if (entryNode.children[1].element.name !== 'value') return false
  return entryNode.element.repetition_type === 'REPEATED'
}

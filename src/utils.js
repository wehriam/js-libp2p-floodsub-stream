'use strict'

const bs58 = require('bs58')

exports = module.exports

/**
 * Check if any member of the first set is also a member
 * of the second set.
 *
 * @param {Set|Array} a
 * @param {Set|Array} b
 * @returns {boolean}
 * @private
 */
exports.anyMatch = (a, b) => {
  let bHas
  if (Array.isArray(b)) {
    bHas = (val) => b.indexOf(val) > -1
  } else {
    bHas = (val) => b.has(val)
  }

  for (let val of a) {
    if (bHas(val)) {
      return true
    }
  }

  return false
}

/**
 * Make everything an array.
 *
 * @param {any} maybeArray
 * @returns {Array}
 * @private
 */
exports.ensureArray = (maybeArray) => {
  if (!Array.isArray(maybeArray)) {
    return [maybeArray]
  }

  return maybeArray
}

exports.normalizeInRpcMessages = (messages) => {
  if (!messages) {
    return messages
  }
  return messages.map((msg) => {
    const m = Object.assign({}, msg)
    if (Buffer.isBuffer(msg.from)) {
      m.from = bs58.encode(msg.from)
    }
    return m
  })
}

exports.normalizeOutRpcMessages = (messages) => {
  if (!messages) {
    return messages
  }
  return messages.map((msg) => {
    const m = Object.assign({}, msg)
    if (typeof msg.from === 'string' || msg.from instanceof String) {
      m.from = bs58.decode(msg.from)
    }
    return m
  })
}

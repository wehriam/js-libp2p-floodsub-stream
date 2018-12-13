'use strict'

const sha256 = require('crypto-digest-sync/sha256')
const arrayBufferToHex = require('array-buffer-to-hex')
const TimeCache = require('time-cache')
const pull = require('pull-stream')
const lp = require('pull-length-prefixed')
const assert = require('assert')

const BaseProtocol = require('./base')
const utils = require('./utils')
const pb = require('./message')
const config = require('./config')

const multicodec = config.multicodec
const ensureArray = utils.ensureArray
const setImmediate = require('async/setImmediate')

/**
 * FloodSub (aka dumbsub is an implementation of pubsub focused on
 * delivering an API for Publish/Subscribe, but with no CastTree Forming
 * (it just floods the network).
 */
class FloodSub extends BaseProtocol {
  /**
   * @param {Object} libp2p
   * @constructor
   */
  constructor (libp2p) {
    super('libp2p:floodsub', multicodec, libp2p)

    /**
     * Time based cache for digests.
     *
     * @type {TimeCache}
     */
    this.cache = new TimeCache()

    /**
     * List of our subscriptions
     * @type {Set<string>}
     */
    this.subscriptions = new Set()
  }

  _onDial (peerInfo, conn, callback) {
    super._onDial(peerInfo, conn, (err) => {
      if (err) return callback(err)
      const idB58Str = peerInfo.id.toB58String()
      const peer = this.peers.get(idB58Str)
      // Immediately send my own subscriptions to the newly established conn
      peer.sendSubscriptions(this.subscriptions)
      setImmediate(() => callback())
    })
  }

  _processConnection (idB58Str, conn, peer) {
    pull(
      conn,
      lp.decode(),
      pull.map((data) => pb.rpc.RPC.decode(data)),
      pull.drain(
        (rpc) => this._onRpc(idB58Str, rpc),
        (err) => this._onConnectionEnd(idB58Str, peer, err)
      )
    )
  }

  _onRpc (idB58Str, rpc) {
    if (!rpc) {
      return
    }

    this.log('rpc from', idB58Str)
    const subs = rpc.subscriptions
    const msgs = rpc.msgs

    if (msgs && msgs.length) {
      this._processRpcMessages(utils.normalizeInRpcMessages(rpc.msgs))
    }

    if (subs && subs.length) {
      const peer = this.peers.get(idB58Str)
      if (peer) {
        peer.updateSubscriptions(subs)
        this.emit('floodsub:subscription-change', peer.info, peer.topics, subs)
      }
    }
  }

  _processRpcMessages (msgs) {
    msgs.forEach((msg) => {
      const digest = arrayBufferToHex(sha256(msg.data))

      // 1. check if I've seen the message, if yes, ignore
      if (this.cache.has(digest)) {
        return
      }

      this.cache.put(digest)

      // 2. emit to self
      this._emitMessages(msg.topicIDs, [msg])

      // 3. propagate msg to others
      this._forwardMessages(msg.topicIDs, [msg])
    })
  }

  _emitMessages (topics, messages) {
    topics.forEach((topic) => {
      if (!this.subscriptions.has(topic)) {
        return
      }

      messages.forEach((message) => {
        this.emit(topic, message)
      })
    })
  }

  _forwardMessages (topics, messages) {
    this.peers.forEach((peer) => {
      if (!peer.isWritable || !utils.anyMatch(peer.topics, topics)) {
        return
      }

      peer.sendMessages(utils.normalizeOutRpcMessages(messages))

      this.log('publish msgs on topics', topics, peer.info.id.toB58String())
    })
  }

  /**
   * Unmounts the floodsub protocol and shuts down every connection
   *
   * @param {Function} callback
   * @returns {undefined}
   *
   */
  stop (callback) {
    super.stop((err) => {
      if (err) return callback(err)
      this.subscriptions = new Set()
      callback()
    })
  }

  /**
   * Publish messages to the given topics.
   *
   * @param {Array<string>|string} topics
   * @param {Array<any>|any} messages
   * @returns {undefined}
   *
   */
  publish (topics, messages) {
    assert(this.started, 'FloodSub is not started')

    this.log('publish', topics, messages)

    topics = ensureArray(topics)
    messages = ensureArray(messages)

    const from = this.libp2p.peerInfo.id.toB58String()

    const buildMessage = (data) => {
      const digest = arrayBufferToHex(sha256(data))
      this.cache.put(digest)
      return {
        from: from,
        data: data,
        topicIDs: topics
      }
    }

    const msgObjects = messages.map(buildMessage)

    // Emit to self if I'm interested
    this._emitMessages(topics, msgObjects)

    // send to all the other peers
    this._forwardMessages(topics, msgObjects)
  }

  /**
   * Subscribe to the given topic(s).
   *
   * @param {Array<string>|string} topics
   * @returns {undefined}
   */
  subscribe (topics) {
    assert(this.started, 'FloodSub is not started')

    topics = ensureArray(topics)

    topics.forEach((topic) => this.subscriptions.add(topic))

    this.peers.forEach((peer) => sendSubscriptionsOnceReady(peer))
    // make sure that FloodSub is already mounted
    function sendSubscriptionsOnceReady (peer) {
      if (peer && peer.isWritable) {
        return peer.sendSubscriptions(topics)
      }
      const onConnection = () => {
        peer.removeListener('connection', onConnection)
        sendSubscriptionsOnceReady(peer)
      }
      peer.on('connection', onConnection)
      peer.once('close', () => peer.removeListener('connection', onConnection))
    }
  }

  /**
   * Unsubscribe from the given topic(s).
   *
   * @param {Array<string>|string} topics
   * @returns {undefined}
   */
  unsubscribe (topics) {
    // Avoid race conditions, by quietly ignoring unsub when shutdown.
    if (!this.started) {
      return
    }

    topics = ensureArray(topics)

    topics.forEach((topic) => this.subscriptions.delete(topic))

    this.peers.forEach((peer) => checkIfReady(peer))
    // make sure that FloodSub is already mounted
    function checkIfReady (peer) {
      if (peer && peer.isWritable) {
        peer.sendUnsubscriptions(topics)
      } else {
        setImmediate(checkIfReady.bind(peer))
      }
    }
  }
}

module.exports = FloodSub

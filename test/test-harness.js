// @ts-check
'use strict'

/** @type {import('assert')} */
const assert = require('assert')
const util = require('util')

const uuid = require('uuid').v4
/** @type {import('@pre-bundled/rimraf')} */
const rimraf = require('@pre-bundled/rimraf')
/** @type {import('@pre-bundled/tape')} */
const tape = require('@pre-bundled/tape')
const tapeHarness = require('tape-harness')
const AWS = require('aws-sdk')

const FakeS3 = require('../index.js')

class TestHarness {
  /**
   * @param {{
   *    buckets?: string[],
   *    waitTimeout?: number,
   *    port?: number
   * }} options
   */
  constructor (options = {}) {
    /** @type {string[]} */
    this.buckets = options.buckets || ['my-bucket']

    const port = 'port' in options ? options.port : undefined
    const opts = {
      prefix: 'foo/',
      waitTimeout: options.waitTimeout,
      buckets: this.buckets,
      port: port
    }

    /** @type {FakeS3} */
    this.server = new FakeS3(opts)

    /** @type {import('aws-sdk').S3 | null} */
    this.s3 = null

    /** @type {string} */
    this.accessKeyId = uuid()

    /** @type {FakeS3 | null} */
    this.cacheServer = null
    /** @type {import('aws-sdk').S3 | null} */
    this.cacheS3 = null
  }

  /** @returns {import('aws-sdk').S3} */
  getS3 () {
    if (!this.s3) throw new Error('bootstrap() first')
    return this.s3
  }

  /**
   * @param {string} cachePath
   * @returns {FakeS3}
   */
  getCacheServer (cachePath) {
    assert(cachePath, 'cachePath required')
    if (this.cacheServer) return this.cacheServer

    this.cacheServer = new FakeS3({
      prefix: '',
      cachePath: cachePath
    })
    return this.cacheServer
  }

  /** @returns {import('aws-sdk').S3} */
  getCacheS3 () {
    if (this.cacheS3) return this.cacheS3
    if (!this.cacheServer) {
      throw new Error('Cache server not started')
    }

    this.cacheS3 = new AWS.S3({
      endpoint: `http://${this.cacheServer.getHostPort()}`,
      sslEnabled: false,
      accessKeyId: this.accessKeyId,
      secretAccessKey: 'abc',
      s3ForcePathStyle: true
    })
    return this.cacheS3
  }

  /** @returns {Promise<void>} */
  async bootstrap () {
    await this.server.bootstrap()

    this.s3 = new AWS.S3({
      endpoint: `http://${this.server.hostPort}`,
      sslEnabled: false,
      accessKeyId: this.accessKeyId,
      secretAccessKey: 'abc',
      s3ForcePathStyle: true
    })
  }

  /**
   * @param {string} bucket
   * @param {string} key
   * @param {Buffer | string} body
   * @returns {Promise<import('aws-sdk').S3.ManagedUpload.SendData>}
   */
  async uploadFileForBucket (bucket, key, body) {
    return this.getS3().upload({
      Bucket: bucket,
      Key: key,
      Body: body
    }).promise()
  }

  /**
   * @param {string} key
   * @param {Buffer | string} body
   * @returns {Promise<import('aws-sdk').S3.ManagedUpload.SendData>}
   */
  async uploadFile (key, body) {
    const bucket = this.buckets[0] || 'my-bucket'
    return this.getS3().upload({
      Bucket: bucket,
      Key: key,
      Body: body
    }).promise()
  }

  /**
   * @param {string} bucket
   * @param {number} count
   * @typedef {import('../index.js').S3ObjectAlias} S3Object
   * @returns {Promise<{ objects: S3Object[] } | null>}
   */
  async waitForFiles (bucket, count) {
    return this.server.waitForFiles(bucket, count)
  }

  /**
   * @param {string} bucket
   * @returns {{ objects: S3Object[] }}
   */
  getFiles (bucket) {
    return this.server.getFiles(bucket)
  }

  async close () {
    await this.server.close()

    if (this.cacheServer) {
      await this.cacheServer.close()
    }

    for (const cachePath of this.server.knownCaches) {
      await util.promisify((
        /** @type {(err?: Error) => void} */ cb
      ) => {
        rimraf(cachePath, cb)
      })()
    }
  }
}

TestHarness.test = tapeHarness(tape, TestHarness)
module.exports = TestHarness

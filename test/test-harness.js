// @ts-check
'use strict'

/** @type {import('assert')} */
const assert = require('assert')
const util = require('util')

const uuid = require('uuid').v4
const rimraf = require('@pre-bundled/rimraf')
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
    this.buckets = options.buckets || ['my-bucket']

    const port = 'port' in options ? options.port : undefined
    const opts = {
      prefix: 'foo/',
      waitTimeout: options.waitTimeout,
      buckets: this.buckets,
      port: port
    }

    this.server = new FakeS3(opts)

    /** @type {import('aws-sdk').S3 | null} */
    this.s3 = null

    this.accessKeyId = uuid()

    this.cacheServer = null
    this.cacheS3 = null
  }

  getS3 () {
    if (!this.s3) throw new Error('bootstrap() first')
    return this.s3
  }

  /**
   * @param {string} cachePath
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
   */
  async waitForFiles (bucket, count) {
    return this.server.waitForFiles(bucket, count)
  }

  /**
   * @param {string} bucket
   */
  async getFiles (bucket) {
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

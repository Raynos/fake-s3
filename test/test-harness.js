'use strict'

const assert = require('assert')
const util = require('util')

const rimraf = require('@pre-bundled/rimraf')
const tape = require('@pre-bundled/tape')
const tapeHarness = require('tape-harness')
const AWS = require('aws-sdk')

const FakeS3 = require('../index.js')

class TestHarness {
  constructor (options = {}) {
    this.buckets = options.buckets || ['my-bucket']

    const opts = {
      prefix: 'foo/',
      waitTimeout: options.waitTimeout,
      buckets: this.buckets
    }
    if ('port' in options) {
      opts.port = options.port
    }

    this.server = new FakeS3(opts)

    /** @type AWS.S3 */
    this.s3 = null

    this.cacheServer = null
    this.cacheS3 = null
  }

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

    this.cacheS3 = new AWS.S3({
      endpoint: `http://${this.cacheServer.hostPort}`,
      sslEnabled: false,
      accessKeyId: '123',
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
      accessKeyId: '123',
      secretAccessKey: 'abc',
      s3ForcePathStyle: true
    })
  }

  async uploadFileForBucket (bucket, key, body) {
    return this.s3.upload({
      Bucket: bucket,
      Key: key,
      Body: body
    }).promise()
  }

  async uploadFile (key, body) {
    const bucket = this.buckets[0] || 'my-bucket'
    return this.s3.upload({
      Bucket: bucket,
      Key: key,
      Body: body
    }).promise()
  }

  async waitForFiles (bucket, count) {
    return this.server.waitForFiles(bucket, count)
  }

  async getFiles (bucket) {
    return this.server.getFiles(bucket)
  }

  async close () {
    await this.server.close()

    if (this.cacheServer) {
      await this.cacheServer.close()
    }

    for (const cachePath of this.server.knownCaches) {
      await util.promisify((cb) => {
        rimraf(cachePath, cb)
      })()
    }
  }
}

/**
 * @type {((
 *   str: string,
 *   opts: object,
 *   fn: (harness: TestHarness, tape: any) => void
 * ) => void) &
 * ((
 *   str: string,
 *   fn: (harness: TestHarness, tape: any) => void
 * ) => void)}
 */
TestHarness.test = tapeHarness(tape, TestHarness)
module.exports = TestHarness

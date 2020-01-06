'use strict'

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

    this.s3 = null
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
  }
}
TestHarness.test = tapeHarness(tape, TestHarness)
module.exports = TestHarness

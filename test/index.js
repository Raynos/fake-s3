'use strict'

const tape = require('tape')
const tapeCluster = require('tape-cluster')
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
TestHarness.test = tapeCluster(tape, TestHarness)

TestHarness.test('fakeS3 is a server', async (harness, assert) => {
  assert.ok(harness.server.hostPort)
})

TestHarness.test('fakeS3 supports uploading & waiting', {
}, async (harness, assert) => {
  const resp = await harness.uploadFile(
    'foo/my-file', 'some text'
  )

  assert.ok(resp)
  assert.ok(resp.ETag)

  const files = await harness.waitForFiles('my-bucket', 1)

  assert.ok(files)
  assert.equal(files.objects.length, 1)

  const obj = files.objects[0]
  assert.equal(obj.bucket, 'my-bucket')
  assert.equal(obj.key, 'foo/my-file')
  assert.equal(obj.content.toString(), 'some text')
})

TestHarness.test('fakeS3 uploading without buckets', {
  buckets: []
}, async (harness, assert) => {
  try {
    await harness.uploadFile(
      'foo/my-file', 'some text'
    )
  } catch (err) {
    assert.equal(err.message,
      'The specified bucket does not exist')
    assert.equal(err.statusCode, 500)
    assert.equal(err.code, 'NoSuchBucket')
    return
  }

  assert.ok(false, 'not reached')
})

TestHarness.test('fakeS3 supports parallel waiting', {
}, async (harness, assert) => {
  const [resp, files] = await Promise.all([
    harness.uploadFile(
      'foo/my-file', 'some text'
    ),
    harness.waitForFiles('my-bucket', 1)
  ])

  assert.ok(resp)
  assert.ok(resp.ETag)

  assert.ok(files)
  assert.equal(files.objects.length, 1)

  const obj = files.objects[0]
  assert.equal(obj.bucket, 'my-bucket')
  assert.equal(obj.key, 'foo/my-file')
  assert.equal(obj.content.toString(), 'some text')
})

TestHarness.test('listen on specific port', {
  port: 14367
}, async (harness, assert) => {
  assert.equal(harness.server.hostPort, 'localhost:14367')
})

TestHarness.test('createBucket not supported', {
}, async (harness, assert) => {
  try {
    await harness.s3.createBucket({
      Bucket: 'example-bucket'
    }).promise()
  } catch (err) {
    assert.equal(err.message, 'invalid url, expected /:bucket/:key')
    assert.equal(err.code, 'InternalError')

    return
  }
  assert.ok(false)
})

TestHarness.test('copyObject not supported', {
}, async (harness, assert) => {
  try {
    await harness.s3.copyObject({
      Bucket: 'example-bucket',
      CopySource: '/foo/my-copy',
      Key: 'my-copy.txt'
    }).promise()
  } catch (err) {
    assert.equal(err.message, 'copyObject() not supported')
    assert.equal(err.code, 'InternalError')

    return
  }
  assert.ok(false)
})

TestHarness.test('uploadPart not supported', {
}, async (harness, assert) => {
  try {
    await harness.s3.uploadPart({
      Body: 'some content',
      Bucket: 'my-bucket',
      Key: 'my-multipart.txt',
      PartNumber: 1,
      UploadId: 'id'
    }).promise()
  } catch (err) {
    assert.equal(err.message, 'putObjectMultipart not supported')
    assert.equal(err.code, 'InternalError')

    return
  }
  assert.ok(false)
})

TestHarness.test('createMultipartUpload not supported', {
}, async (harness, assert) => {
  try {
    await harness.s3.createMultipartUpload({
      Bucket: 'my-bucket',
      Key: 'my-multipart.txt'
    }).promise()
  } catch (err) {
    assert.equal(
      err.message,
      'url not supported: POST /my-bucket/my-multipart.txt?uploads'
    )
    assert.equal(err.code, 'InternalError')

    return
  }
  assert.ok(false)
})

TestHarness.test('getFiles() for empty bucket', {
}, async (harness, assert) => {
  const files = await harness.getFiles('my-bucket')
  assert.equal(files.objects.length, 0)
})

TestHarness.test('getFiles() for non-existant bucket', {
}, async (harness, assert) => {
  const files = await harness.getFiles('no-bucket')
  assert.equal(files.objects.length, 0)
})

TestHarness.test('waitForFiles() timeout', {
  waitTimeout: 150
}, async (harness, assert) => {
  const start = Date.now()
  const files = await harness.waitForFiles('my-bucket', 1)
  const end = Date.now()
  assert.equal(files, null)

  assert.ok(end - start > 150)
})

// TODO: test for s3.listObjectsV2

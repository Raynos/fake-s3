'use strict'

var test = require('tape')
const util = require('util')
const AWS = require('aws-sdk')

var FakeS3 = require('../index.js')

test('fakeS3 is a server', async (assert) => {
  const server = new FakeS3({
    prefix: 'foo/',
    buckets: ['my-bucket']
  })

  await util.promisify((cb) => {
    server.bootstrap(cb)
  })()

  assert.ok(server.hostPort)

  server.close()
  assert.end()
})

test('fakeS3 supports uploading & waiting', async (assert) => {
  const server = new FakeS3({
    prefix: 'foo/',
    buckets: ['my-bucket']
  })

  await util.promisify((cb) => {
    server.bootstrap(cb)
  })()

  const s3 = new AWS.S3({
    endpoint: `http://${server.hostPort}`,
    sslEnabled: false,
    accessKeyId: '123',
    secretAccessKey: 'abc',
    s3ForcePathStyle: true
  })

  const resp = await s3.upload({
    Bucket: 'my-bucket',
    Key: 'foo/my-file',
    Body: 'some text'
  }).promise()

  assert.ok(resp)
  assert.ok(resp.ETag)

  const files = await util.promisify((cb) => {
    server.waitForFiles('my-bucket', 1, cb)
  })()

  assert.ok(files)
  assert.equal(files.objects.length, 1)

  const obj = files.objects[0]
  assert.equal(obj.bucket, 'my-bucket')
  assert.equal(obj.key, 'foo/my-file')
  assert.equal(obj.content.toString(), 'some text')

  server.close()
  assert.end()
})

test('fakeS3 supports parallel waiting', async (assert) => {
  const server = new FakeS3({
    prefix: 'foo/',
    buckets: ['my-bucket']
  })

  await util.promisify((cb) => {
    server.bootstrap(cb)
  })()

  const s3 = new AWS.S3({
    endpoint: `http://${server.hostPort}`,
    sslEnabled: false,
    accessKeyId: '123',
    secretAccessKey: 'abc',
    s3ForcePathStyle: true
  })

  const [resp, files] = await Promise.all([
    s3.upload({
      Bucket: 'my-bucket',
      Key: 'foo/my-file',
      Body: 'some text'
    }).promise(),
    util.promisify((cb) => {
      server.waitForFiles('my-bucket', 1, cb)
    })()
  ])

  assert.ok(resp)
  assert.ok(resp.ETag)

  assert.ok(files)
  assert.equal(files.objects.length, 1)

  const obj = files.objects[0]
  assert.equal(obj.bucket, 'my-bucket')
  assert.equal(obj.key, 'foo/my-file')
  assert.equal(obj.content.toString(), 'some text')

  server.close()
  assert.end()
})

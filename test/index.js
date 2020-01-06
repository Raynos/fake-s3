'use strict'

const { test } = require('./test-harness')

test('fakeS3 is a server', async (harness, assert) => {
  assert.ok(harness.server.hostPort)
})

test('fakeS3 supports uploading & waiting', {
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

test('fakeS3 uploading without buckets', {
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

test('fakeS3 supports parallel waiting', {
}, async (harness, assert) => {
  const results = await Promise.all([
    harness.waitForFiles('my-bucket', 2),
    harness.uploadFile(
      'foo/my-file', 'some text'
    ),
    harness.uploadFile(
      'bar/my-file', 'some text'
    ),
    harness.uploadFile(
      'baz/my-file', 'some text'
    ),
    harness.uploadFile(
      'foo/my-file2', 'some text2'
    )
  ])

  const files = results[0]
  const resp = results[1]

  assert.ok(resp)
  assert.ok(resp.ETag)

  assert.ok(files)
  assert.equal(files.objects.length, 2)

  const obj = files.objects[0]
  assert.equal(obj.bucket, 'my-bucket')
  assert.equal(obj.key, 'foo/my-file')
  assert.equal(obj.content.toString(), 'some text')

  const allFiles = await harness.getFiles('my-bucket')
  assert.equal(allFiles.objects.length, 2)
})

test('fakeS3 supports prefix', {
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

test('listen on specific port', {
  port: 14367
}, async (harness, assert) => {
  assert.equal(harness.server.hostPort, 'localhost:14367')
})

test('createBucket not supported', {
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

test('copyObject not supported', {
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

test('uploadPart not supported', {
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

test('createMultipartUpload not supported', {
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

test('getFiles() for empty bucket', {
}, async (harness, assert) => {
  const files = await harness.getFiles('my-bucket')
  assert.equal(files.objects.length, 0)
})

test('getFiles() for non-existant bucket', {
}, async (harness, assert) => {
  const files = await harness.getFiles('no-bucket')
  assert.equal(files.objects.length, 0)
})

test('waitForFiles() timeout', {
  waitTimeout: 150
}, async (harness, assert) => {
  const start = Date.now()
  const files = await harness.waitForFiles('my-bucket', 1)
  const end = Date.now()
  assert.equal(files, null)

  assert.ok(end - start > 150)
})

test('getObject not supported', {
}, async (harness, assert) => {
  try {
    await harness.s3.getObject({
      Bucket: 'my-bucket',
      Key: 'my-multipart.txt'
    }).promise()
  } catch (err) {
    assert.equal(err.message, 'invalid url, expected /:bucket')
    assert.equal(err.code, 'InternalError')

    return
  }
  assert.ok(false)
})

test('')

test('listObjectsV2 query', {
}, async (harness, assert) => {
  const resp = await harness.uploadFile(
    'foo/my-file', 'some text'
  )

  assert.ok(resp)
  assert.ok(resp.ETag)

  const resp2 = await harness.s3.listObjectsV2({
    Bucket: 'my-bucket',
    MaxKeys: 100
  }).promise()

  assert.ok(resp2)
  assert.equal(resp2.Contents.length, 1)
  assert.equal(resp2.Contents[0].Key, 'foo/my-file')
  assert.equal(resp2.Contents[0].Size, 9)
  assert.equal(resp2.Name, 'my-bucket')
})

test('listObjectsV2 query on non-existant bucket', {
}, async (harness, assert) => {
  const resp = await harness.uploadFile(
    'foo/my-file', 'some text'
  )

  assert.ok(resp)
  assert.ok(resp.ETag)

  try {
    await harness.s3.listObjectsV2({
      Bucket: 'my-bucket2',
      MaxKeys: 100
    }).promise()
  } catch (err) {
    assert.equal(err.message,
      'The specified bucket does not exist')
    assert.equal(err.statusCode, 500)
    assert.equal(err.code, 'NoSuchBucket')
    return
  }

  assert.ok(false, 'not reached')
})

test('listBuckets', async (harness, assert) => {
  const data = await harness.s3.listBuckets({}).promise()

  assert.ok(data)
  assert.ok(data.Owner)
  assert.deepEqual(data.Buckets, [{
    Name: 'my-bucket',
    CreationDate: data.Buckets[0].CreationDate
  }])
  assert.deepEqual(data.Owner, {
    ID: '1',
    DisplayName: 'admin'
  })
})

test('listBuckets 5', {
  buckets: [
    'bucket1', 'bucket2', 'bucket3', 'bucket4', 'bucket5'
  ]
}, async (harness, assert) => {
  const data = await harness.s3.listBuckets({}).promise()

  assert.ok(data)
  assert.ok(data.Owner)
  assert.deepEqual(data.Buckets, [{
    Name: 'bucket1',
    CreationDate: data.Buckets[0].CreationDate
  }, {
    Name: 'bucket2',
    CreationDate: data.Buckets[0].CreationDate
  }, {
    Name: 'bucket3',
    CreationDate: data.Buckets[0].CreationDate
  }, {
    Name: 'bucket4',
    CreationDate: data.Buckets[0].CreationDate
  }, {
    Name: 'bucket5',
    CreationDate: data.Buckets[0].CreationDate
  }])
})

'use strict'

const path = require('path')
const os = require('os')

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

test('can cache buckets', {
  buckets: ['buckets1', 'buckets2', 'buckets3']
}, async (harness, assert) => {
  const cachePath = path.join(
    os.tmpdir(), `test-fake-s3-${cuuid()}`
  )

  const buckets = await harness.s3.listBuckets().promise()
  // Test double caching is indempotent.
  await harness.server.cacheBucketsToDisk(cachePath, buckets)
  await harness.server.cacheBucketsToDisk(cachePath, buckets)

  const server2 = harness.getCacheServer(cachePath)
  await server2.bootstrap()

  const cacheS3 = harness.getCacheS3()

  const cacheBuckets = await cacheS3.listBuckets().promise()
  assert.deepEqual(buckets, cacheBuckets)

  assert.deepEqual(cacheBuckets.Buckets, [{
    Name: 'buckets1',
    CreationDate: cacheBuckets.Buckets[0].CreationDate
  }, {
    Name: 'buckets2',
    CreationDate: cacheBuckets.Buckets[1].CreationDate
  }, {
    Name: 'buckets3',
    CreationDate: cacheBuckets.Buckets[2].CreationDate
  }])

  assert.end()
})

test('can cache buckets', {
  buckets: ['bucket1', 'bucket2']
}, async (harness, assert) => {
  const cachePath = path.join(
    os.tmpdir(), `test-fake-s3-${cuuid()}`
  )

  await harness.uploadFileForBucket(
    'bucket1', 'foo/my-file', 'some foo text'
  )
  await harness.uploadFileForBucket(
    'bucket1', 'bar/my-file', 'some bar text'
  )

  await harness.uploadFileForBucket(
    'bucket2', 'foo/foo', 'some foo text 123'
  )
  await harness.uploadFileForBucket(
    'bucket2', 'bar/bar', 'some bar text 123456789'
  )
  await harness.uploadFileForBucket(
    'bucket2', 'baz/baz', 'some baz text 123456789000'
  )

  const buckets = await harness.s3.listBuckets().promise()
  await harness.server.cacheBucketsToDisk(cachePath, buckets)

  const objects1 = await harness.s3.listObjectsV2({
    Bucket: 'bucket1'
  }).promise()
  await harness.server.cacheObjectsToDisk(
    cachePath, 'bucket1', objects1
  )

  const objects2 = await harness.s3.listObjectsV2({
    Bucket: 'bucket2'
  }).promise()
  await harness.server.cacheObjectsToDisk(
    cachePath, 'bucket2', objects2
  )

  const server2 = harness.getCacheServer(cachePath)
  await server2.bootstrap()

  const cacheS3 = harness.getCacheS3()

  const cacheBuckets = await cacheS3.listBuckets().promise()
  assert.deepEqual(cacheBuckets.Buckets, [{
    Name: 'bucket1',
    CreationDate: cacheBuckets.Buckets[0].CreationDate
  }, {
    Name: 'bucket2',
    CreationDate: cacheBuckets.Buckets[1].CreationDate
  }])

  const cobjects1 = await cacheS3.listObjectsV2({
    Bucket: 'bucket1'
  }).promise()
  assert.equal(cobjects1.Name, 'bucket1')
  assert.equal(cobjects1.KeyCount, 2)

  assert.deepEqual(cobjects1.Contents, [{
    Key: 'foo/my-file',
    ETag: '385da0ff8300f1adbd45b2f9dea6808f',
    Size: 13,
    StorageClass: 'STANDARD'
  }, {
    Key: 'bar/my-file',
    ETag: '4a6509a66ec6815a287a01ee32e44dbc',
    Size: 13,
    StorageClass: 'STANDARD'
  }])

  const cobjects2 = await cacheS3.listObjectsV2({
    Bucket: 'bucket2'
  }).promise()
  assert.equal(cobjects2.Name, 'bucket2')
  assert.equal(cobjects2.KeyCount, 3)

  assert.deepEqual(cobjects2.Contents, [{
    Key: 'foo/foo',
    ETag: '0ceba125bd0b23ccb487aeb3c29a6783',
    Size: 17,
    StorageClass: 'STANDARD'
  }, {
    Key: 'bar/bar',
    ETag: 'c766bdc746dee5d795f3914e5698a3dd',
    Size: 23,
    StorageClass: 'STANDARD'
  }, {
    Key: 'baz/baz',
    ETag: '67f258e01a0e0a6aa4a2853eaaf20360',
    Size: 26,
    StorageClass: 'STANDARD'
  }])

  assert.end()
})

function cuuid () {
  const str = (
    Date.now().toString(16) +
    Math.random().toString(16).slice(2) +
    Math.random().toString(16).slice(2)
  ).slice(0, 32)
  return str.slice(0, 8) + '-' + str.slice(8, 12) + '-' +
    str.slice(12, 16) + '-' + str.slice(16, 20) + '-' +
    str.slice(20)
}

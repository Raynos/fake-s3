'use strict'

const path = require('path')
const os = require('os')
/** @type {import('assert')} */
const coreAssert = require('assert')

/**
 * @typedef {{
 *    message: string,
 *    statusCode: number,
 *    code: string
 * }} StatusError
 */

const { test } = require('./test-harness')

test('fakeS3 is a server', (harness, assert) => {
  assert.ok(harness.server.hostPort)
  assert.end()
})

test('fakeS3 supports uploading & waiting', {
}, async (harness, assert) => {
  const resp = await harness.uploadFile(
    'foo/my-file', 'some text'
  )

  assert.ok(resp)
  assert.ok(resp.ETag)

  const files = await harness.waitForFiles('my-bucket', 1)

  coreAssert(files, 'files must exist')
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
  } catch (maybeErr) {
    /* eslint-disable-next-line @typescript-eslint/no-unsafe-assignment */
    const err = /** @type {StatusError} */ (maybeErr)
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

  coreAssert(files)
  assert.ok(files)
  assert.equal(files.objects.length, 2)

  const obj = files.objects[0]
  assert.equal(obj.bucket, 'my-bucket')
  assert.equal(obj.key, 'foo/my-file')
  assert.equal(obj.content.toString(), 'some text')

  const allFiles = harness.getFiles('my-bucket')
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

  coreAssert(files)
  assert.ok(files)
  assert.equal(files.objects.length, 1)

  const obj = files.objects[0]
  assert.equal(obj.bucket, 'my-bucket')
  assert.equal(obj.key, 'foo/my-file')
  assert.equal(obj.content.toString(), 'some text')
})

test('listen on specific port', {
  port: 14367
}, (harness, assert) => {
  assert.equal(harness.server.hostPort, 'localhost:14367')
  assert.end()
})

test('createBucket not supported', {
}, async (harness, assert) => {
  try {
    await harness.getS3().createBucket({
      Bucket: 'example-bucket'
    }).promise()
  } catch (maybeErr) {
    /* eslint-disable-next-line @typescript-eslint/no-unsafe-assignment */
    const err = /** @type {StatusError} */ (maybeErr)
    assert.equal(err.message, 'invalid url, expected /:bucket/:key')
    assert.equal(err.code, 'InternalError')

    return
  }
  assert.ok(false)
})

test('copyObject not supported', {
}, async (harness, assert) => {
  try {
    await harness.getS3().copyObject({
      Bucket: 'example-bucket',
      CopySource: '/foo/my-copy',
      Key: 'my-copy.txt'
    }).promise()
  } catch (maybeErr) {
    /* eslint-disable-next-line @typescript-eslint/no-unsafe-assignment */
    const err = /** @type {StatusError} */ (maybeErr)
    assert.equal(err.message, 'copyObject() not supported')
    assert.equal(err.code, 'InternalError')

    return
  }
  assert.ok(false)
})

test('uploadPart not supported', {
}, async (harness, assert) => {
  try {
    await harness.getS3().uploadPart({
      Body: 'some content',
      Bucket: 'my-bucket',
      Key: 'my-multipart.txt',
      PartNumber: 1,
      UploadId: 'id'
    }).promise()
  } catch (maybeErr) {
    /* eslint-disable-next-line @typescript-eslint/no-unsafe-assignment */
    const err = /** @type {StatusError} */ (maybeErr)
    assert.equal(err.message, 'putObjectMultipart not supported')
    assert.equal(err.code, 'InternalError')

    return
  }
  assert.ok(false)
})

test('createMultipartUpload not supported', {
}, async (harness, assert) => {
  try {
    await harness.getS3().createMultipartUpload({
      Bucket: 'my-bucket',
      Key: 'my-multipart.txt'
    }).promise()
  } catch (maybeErr) {
    /* eslint-disable-next-line @typescript-eslint/no-unsafe-assignment */
    const err = /** @type {StatusError} */ (maybeErr)
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
}, (harness, assert) => {
  const files = harness.getFiles('my-bucket')
  assert.equal(files.objects.length, 0)
  assert.end()
})

test('getFiles() for non-existant bucket', {
}, (harness, assert) => {
  const files = harness.getFiles('no-bucket')
  assert.equal(files.objects.length, 0)
  assert.end()
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
    await harness.getS3().getObject({
      Bucket: 'my-bucket',
      Key: 'my-multipart.txt'
    }).promise()
  } catch (maybeErr) {
    /* eslint-disable-next-line @typescript-eslint/no-unsafe-assignment */
    const err = /** @type {StatusError} */ (maybeErr)
    assert.equal(err.message, 'invalid url, expected /:bucket')
    assert.equal(err.code, 'InternalError')

    return
  }
  assert.ok(false)
})

test('listObjectsV2 query', {
}, async (harness, assert) => {
  const resp = await harness.uploadFile(
    'foo/my-file', 'some text'
  )

  assert.ok(resp)
  assert.ok(resp.ETag)

  const resp2 = await harness.getS3().listObjectsV2({
    Bucket: 'my-bucket',
    MaxKeys: 100
  }).promise()

  coreAssert(resp2.Contents)
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
    await harness.getS3().listObjectsV2({
      Bucket: 'my-bucket2',
      MaxKeys: 100
    }).promise()
  } catch (maybeErr) {
    /* eslint-disable-next-line @typescript-eslint/no-unsafe-assignment */
    const err = /** @type {StatusError} */ (maybeErr)
    assert.equal(err.message,
      'The specified bucket does not exist')
    assert.equal(err.statusCode, 500)
    assert.equal(err.code, 'NoSuchBucket')
    return
  }

  assert.ok(false, 'not reached')
})

test('listBuckets', async (harness, assert) => {
  const data = await harness.getS3().listBuckets().promise()

  assert.ok(data)
  assert.ok(data.Owner)
  coreAssert(data.Buckets)
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
  const data = await harness.getS3().listBuckets().promise()

  assert.ok(data)
  assert.ok(data.Owner)
  coreAssert(data.Buckets)
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

  const s3 = harness.getS3()
  const buckets = await s3.listBuckets().promise()
  const creds = s3.config.credentials
  const accessKeyId = (creds && creds.accessKeyId) || ''

  // Test double caching is indempotent.
  await harness.server.cacheBucketsToDisk(
    cachePath, accessKeyId, buckets
  )
  await harness.server.cacheBucketsToDisk(
    cachePath, accessKeyId, buckets
  )

  const server2 = harness.getCacheServer(cachePath)
  await server2.bootstrap()

  const cacheS3 = harness.getCacheS3()

  const cacheBuckets = await cacheS3.listBuckets().promise()
  assert.deepEqual(buckets, cacheBuckets)

  coreAssert(cacheBuckets.Buckets)
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

test('list objects with prefix', async (harness, assert) => {
  const testFiles = [
    'about/index.html',
    'style/styles.css',
    'images/wizard.png',
    'images/sample.png',
    'images/logo.png',
    'images/art/solarsystem.svg',
    'images/art/planet.svg',
    'index.css',
    'index.html'
  ]

  for (const file of testFiles) {
    await harness.uploadFile(file, file)
  }

  const resp = await harness.getS3().listObjectsV2({
    Bucket: 'my-bucket',
    Prefix: 'images/'
  }).promise()

  assert.equal(resp.KeyCount, 5)
  assert.equal(resp.Name, 'my-bucket')
  assert.equal(resp.MaxKeys, 1000)
  assert.equal(resp.IsTruncated, false)
  assert.ok(resp.Contents)
  coreAssert(resp.Contents)
  assert.equal(resp.Contents.length, 5)

  assert.deepEqual(resp.Contents.map(o => o.Key), [
    'images/art/planet.svg',
    'images/art/solarsystem.svg',
    'images/logo.png',
    'images/sample.png',
    'images/wizard.png'
  ])

  assert.end()
})

test('list objects with startAfter', async (harness, assert) => {
  const testFiles = [
    'about/index.html',
    'style/styles.css',
    'images/wizard.png',
    'images/sample.png',
    'images/logo.png',
    'images/art/solarsystem.svg',
    'images/art/planet.svg',
    'index.css',
    'index.html'
  ]

  for (const file of testFiles) {
    await harness.uploadFile(file, file)
  }

  const resp = await harness.getS3().listObjectsV2({
    Bucket: 'my-bucket',
    StartAfter: 'images/logo.png'
  }).promise()

  assert.equal(resp.KeyCount, 5)
  assert.equal(resp.Name, 'my-bucket')
  assert.equal(resp.MaxKeys, 1000)
  assert.equal(resp.IsTruncated, false)
  assert.ok(resp.Contents)
  coreAssert(resp.Contents)
  assert.equal(resp.Contents.length, 5)

  assert.deepEqual(resp.Contents.map(o => o.Key), [
    'images/sample.png',
    'images/wizard.png',
    'index.css',
    'index.html',
    'style/styles.css'
  ])

  assert.end()
})

test('list objects with continuationToken', {
}, async (harness, assert) => {
  const testFiles = [
    'about/index.html',
    'style/styles.css',
    'images/wizard.png',
    'images/sample.png',
    'images/logo.png',
    'images/art/solarsystem.svg',
    'images/art/planet.svg',
    'index.css',
    'index.html'
  ]

  for (const file of testFiles) {
    await harness.uploadFile(file, file)
  }

  const resp = await harness.getS3().listObjectsV2({
    Bucket: 'my-bucket',
    MaxKeys: 3
  }).promise()

  assert.equal(resp.KeyCount, 3)
  assert.equal(resp.MaxKeys, 3)
  assert.equal(resp.Name, 'my-bucket')
  assert.equal(resp.IsTruncated, true)
  coreAssert(resp.Contents)
  assert.deepEqual(resp.Contents.map(o => o.Key), [
    'about/index.html',
    'images/art/planet.svg',
    'images/art/solarsystem.svg'
  ])
  assert.ok(resp.NextContinuationToken)
  assert.notOk(resp.ContinuationToken)

  const resp2 = await harness.getS3().listObjectsV2({
    Bucket: 'my-bucket',
    MaxKeys: 3,
    ContinuationToken: resp.NextContinuationToken
  }).promise()

  assert.equal(resp2.KeyCount, 3)
  assert.equal(resp2.MaxKeys, 3)
  assert.equal(resp2.Name, 'my-bucket')
  assert.equal(resp2.IsTruncated, true)
  coreAssert(resp2.Contents)
  assert.deepEqual(resp2.Contents.map(o => o.Key), [
    'images/logo.png',
    'images/sample.png',
    'images/wizard.png'
  ])
  assert.ok(resp2.ContinuationToken)
  assert.ok(resp2.NextContinuationToken)

  const resp3 = await harness.getS3().listObjectsV2({
    Bucket: 'my-bucket',
    MaxKeys: 3,
    ContinuationToken: resp2.NextContinuationToken
  }).promise()

  assert.equal(resp3.KeyCount, 3)
  assert.equal(resp3.MaxKeys, 3)
  assert.equal(resp3.Name, 'my-bucket')
  assert.equal(resp3.IsTruncated, false)
  coreAssert(resp3.Contents)
  assert.deepEqual(resp3.Contents.map(o => o.Key), [
    'index.css',
    'index.html',
    'style/styles.css'
  ])
  assert.ok(resp3.ContinuationToken)
  assert.notOk(resp3.NextContinuationToken)

  assert.end()
})

test('list objects with delimiter', async (harness, assert) => {
  const testFiles = [
    'about/index.html',
    'style/styles.css',
    'images/wizard.png',
    'images/sample.png',
    'images/logo.png',
    'images/art/solarsystem.svg',
    'images/art/planet.svg',
    '.DS_Store',
    'index.css',
    'index.html'
  ]

  for (const file of testFiles) {
    await harness.uploadFile(file, file)
  }

  const resp = await harness.getS3().listObjectsV2({
    Bucket: 'my-bucket',
    Delimiter: '/'
  }).promise()

  assert.equal(resp.IsTruncated, false)
  assert.equal(resp.Name, 'my-bucket')
  assert.equal(resp.Prefix, '')
  assert.equal(resp.Delimiter, '/')
  assert.equal(resp.MaxKeys, 1000)
  assert.equal(resp.KeyCount, 6)

  assert.deepEqual(resp.CommonPrefixes, [{
    Prefix: 'about/'
  }, {
    Prefix: 'images/'
  }, {
    Prefix: 'style/'
  }])
  coreAssert(resp.Contents)
  assert.deepEqual(resp.Contents.map(o => o.Key), [
    '.DS_Store',
    'index.css',
    'index.html'
  ])

  const resp2 = await harness.getS3().listObjectsV2({
    Bucket: 'my-bucket',
    Delimiter: '/',
    MaxKeys: 2
  }).promise()

  assert.equal(resp2.IsTruncated, true)
  assert.equal(resp2.Name, 'my-bucket')
  assert.equal(resp2.Prefix, '')
  assert.equal(resp2.Delimiter, '/')
  assert.equal(resp2.MaxKeys, 2)
  assert.equal(resp2.KeyCount, 2)

  assert.deepEqual(resp2.CommonPrefixes, [{
    Prefix: 'about/'
  }])
  coreAssert(resp2.Contents)
  assert.deepEqual(resp2.Contents.map(o => o.Key), [
    '.DS_Store'
  ])
  assert.ok(resp2.NextContinuationToken)

  assert.end()
})

test('list objects with prefix & delimiter',
  async (harness, assert) => {
    const testFiles = [
      'about/index.html',
      'style/styles.css',
      'images/wizard.png',
      'images/sample.png',
      'images/logo.png',
      'images/art/solarsystem.svg',
      'images/art/planet.svg',
      '.DS_Store',
      'index.css',
      'index.html'
    ]

    for (const file of testFiles) {
      await harness.uploadFile(file, file)
    }

    const resp = await harness.getS3().listObjectsV2({
      Bucket: 'my-bucket',
      Delimiter: '/',
      Prefix: 'images/'
    }).promise()

    assert.equal(resp.IsTruncated, false)
    assert.equal(resp.Name, 'my-bucket')
    assert.equal(resp.Prefix, 'images/')
    assert.equal(resp.Delimiter, '/')
    assert.equal(resp.MaxKeys, 1000)
    assert.equal(resp.KeyCount, 4)

    assert.deepEqual(resp.CommonPrefixes, [{
      Prefix: 'images/art/'
    }])
    coreAssert(resp.Contents)
    assert.deepEqual(resp.Contents.map(o => o.Key), [
      'images/logo.png',
      'images/sample.png',
      'images/wizard.png'
    ])

    const resp2 = await harness.getS3().listObjectsV2({
      Bucket: 'my-bucket',
      Delimiter: '/',
      Prefix: 'images'
    }).promise()

    assert.equal(resp2.IsTruncated, false)
    assert.equal(resp2.Name, 'my-bucket')
    assert.equal(resp2.Prefix, 'images')
    assert.equal(resp2.Delimiter, '/')
    assert.equal(resp2.MaxKeys, 1000)
    assert.equal(resp2.KeyCount, 1)

    assert.deepEqual(resp2.CommonPrefixes, [{
      Prefix: 'images/'
    }])
    coreAssert(resp2.Contents)
    assert.deepEqual(resp2.Contents.map(o => o.Key), [])

    assert.end()
  })

test('can cache objects', {
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

  const s3 = harness.getS3()
  const buckets = await s3.listBuckets().promise()
  const creds = s3.config.credentials
  const accessKeyId = (creds && creds.accessKeyId) || ''

  await harness.server.cacheBucketsToDisk(cachePath, accessKeyId, buckets)

  const objects1 = await harness.getS3().listObjectsV2({
    Bucket: 'bucket1'
  }).promise()
  await harness.server.cacheObjectsToDisk(
    cachePath, accessKeyId, 'bucket1', objects1
  )

  const objects2 = await harness.getS3().listObjectsV2({
    Bucket: 'bucket2'
  }).promise()
  await harness.server.cacheObjectsToDisk(
    cachePath, accessKeyId, 'bucket2', objects2
  )

  const server2 = harness.getCacheServer(cachePath)
  await server2.bootstrap()

  const cacheS3 = harness.getCacheS3()

  const cacheBuckets = await cacheS3.listBuckets().promise()
  coreAssert(cacheBuckets.Buckets)
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

  coreAssert(cobjects1.Contents)
  assert.deepEqual(cobjects1.Contents, [{
    Key: 'bar/my-file',
    ETag: '4a6509a66ec6815a287a01ee32e44dbc',
    LastModified: cobjects1.Contents[0].LastModified,
    Size: 13,
    StorageClass: 'STANDARD'
  }, {
    Key: 'foo/my-file',
    ETag: '385da0ff8300f1adbd45b2f9dea6808f',
    LastModified: cobjects1.Contents[1].LastModified,
    Size: 13,
    StorageClass: 'STANDARD'
  }])
  assert.ok(cobjects1.Contents[0].LastModified)
  assert.ok(cobjects1.Contents[1].LastModified)

  const cobjects2 = await cacheS3.listObjectsV2({
    Bucket: 'bucket2'
  }).promise()
  assert.equal(cobjects2.Name, 'bucket2')
  assert.equal(cobjects2.KeyCount, 3)

  coreAssert(cobjects2.Contents)
  assert.deepEqual(cobjects2.Contents, [{
    Key: 'bar/bar',
    ETag: 'c766bdc746dee5d795f3914e5698a3dd',
    LastModified: cobjects2.Contents[0].LastModified,
    Size: 23,
    StorageClass: 'STANDARD'
  }, {
    Key: 'baz/baz',
    ETag: '67f258e01a0e0a6aa4a2853eaaf20360',
    LastModified: cobjects2.Contents[1].LastModified,
    Size: 26,
    StorageClass: 'STANDARD'
  }, {
    Key: 'foo/foo',
    ETag: '0ceba125bd0b23ccb487aeb3c29a6783',
    LastModified: cobjects2.Contents[2].LastModified,
    Size: 17,
    StorageClass: 'STANDARD'
  }])

  assert.end()
})

/** @returns {string} */
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

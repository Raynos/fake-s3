'use strict'

const path = require('path')
const AWS = require('aws-sdk')

const FakeS3 = require('../index.js')

async function main () {
  const fakeS3 = new FakeS3({
    prefix: '',
    buckets: []
  })
  const s3 = new AWS.S3()

  const cachePath = path.join(__dirname, '..', 'fixtures')
  await fakeS3.populateFromCache(cachePath)

  if (process.argv[2] !== 'download') {
    console.log('buckets', [...fakeS3._buckets.keys()])

    let totalObjects = 0
    for (const b of fakeS3._buckets.values()) {
      totalObjects += b._objects.size
    }
    console.log('objects count', totalObjects)
    return
  }

  // Cache buckets
  const buckets = await s3.listBuckets().promise()
  await fakeS3.cacheBucketsToDisk(cachePath, buckets)

  await fakeS3.populateFromCache(cachePath)

  // Cache objects
  for (const b of buckets.Buckets) {
    let bucketName = b.Name
    let allObjects = []
    let resp

    let maxReq = 15
    let reqCount = 0

    do {
      console.log('fetching objects',
        bucketName, resp && resp.NextContinuationToken)

      const params = {
        Bucket: bucketName,
        ContinuationToken: resp && resp.NextContinuationToken
      }

      if (reqCount >= maxReq) {
        const prefix = longestCommonPrefix(
          resp.Contents.map(c => c.Key)
        )

        const parts = prefix.split('/')
        parts.pop()
        const folderPrefix = parts.join('/')

        if (folderPrefix.length >= 2) {
          params.StartAfter = folderPrefix + '\xFF'
          delete params.ContinuationToken
          reqCount = 0
        }
      }

      reqCount++
      resp = await s3.listObjectsV2(params).promise()

      console.log('fetched', resp.Contents.length,
        resp.Contents.slice(0, 5).map(c => c.Key), reqCount)

      allObjects.push(...resp.Contents)
    } while (resp && resp.IsTruncated)

    await fakeS3.cacheObjectsToDisk(cachePath, bucketName, {
      Contents: allObjects
    })
  }
}

main().then(null, (err) => {
  process.nextTick(() => {
    throw err
  })
})

function longestCommonPrefix (strs) {
  if (!strs) {
    return ''
  }

  let smallest = strs[0]
  let largest = strs[0]
  for (let i = 1; i < strs.length; i++) {
    let s = strs[i]
    if (s > largest) {
      largest = s
    }
    if (s < smallest) {
      smallest = s
    }
  }
  for (let i = 0; i < smallest.length; i++) {
    if (smallest[i] !== largest[i]) {
      return smallest.substr(0, i)
    }
  }

  return ''
}

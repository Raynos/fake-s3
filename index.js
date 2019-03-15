'use strict'

const os = require('os')
const path = require('path')
const rawBody = require('raw-body')
const uuid = require('uuid')
const S3rver = require('s3rver')
const AWS = require('aws-sdk')
const FileSystemStore = require('s3rver/lib/stores/filesystem.js')
const assert = require('assert')

class FakeS3 {
  constructor (options) {
    assert(options, 'options required')
    assert(options.prefix, 'options.prefix required')

    this.directory = path.join(os.tmpdir(), uuid(), 's3rver')
    const s3rverOpts = {
      port: 'port' in options ? options.port : 0,
      hostname: options.hostname || 'localhost',
      silent: 'silent' in options ? options.silent : true,
      directory: this.directory
    }

    this.s3rver = new S3rver(s3rverOpts)
    this.httpServer = null
    this.hostPort = null
    this.prefix = options.prefix
    this.buckets = options.buckets || []
    this.store = new FileSystemStore(this.directory)
  }

  bootstrap (cb) {
    this.httpServer = this.s3rver.run((err, host, port) => {
      if (err) {
        return cb(err)
      }

      this.hostPort = host + ':' + port
      this.setupBuckets(cb)
    })
  }

  getFiles (bucket, cb) {
    const self = this
    this.store.listObjects(bucket, {
      prefix: this.prefix
    }, onObjects)

    function onObjects (err, info) {
      if (err) {
        return cb(err)
      }

      if (info.objects.length === 0) {
        return cb(err, info)
      }

      let counter = info.objects.length
      for (const o of info.objects) {
        self.store.getObject(bucket, o.key, onObject)
      }

      function onObject (err, objectInfo) {
        if (err) {
          return cb(err)
        }

        const stream = objectInfo.content
        rawBody(stream, onBody)

        function onBody (err, body) {
          if (err) {
            return cb(err)
          }

          for (const o of info.objects) {
            if (o.key === objectInfo.key) {
              o.content = body
              break
            }
          }

          if (--counter === 0) {
            cb(null, info)
          }
        }
      }
    }
  }

  waitForFiles (bucket, count, cb) {
    const self = this
    const maxTimeout = 5 * 1000
    const deadline = Date.now() + maxTimeout
    attempt()

    function attempt () {
      if (Date.now() > deadline) {
        return cb(new Error(
          'timeout waiting for objects in s3rver'
        ))
      }

      self.getFiles(bucket, onBuckets)
    }

    function onBuckets (err, info) {
      if (err) {
        return cb(err)
      }

      if (info.objects.length === count) {
        return cb(err, info)
      }

      setTimeout(attempt, 100)
    }
  }

  setupBuckets (cb) {
    if (this.buckets.length === 0) {
      return cb()
    }

    const s3 = new AWS.S3({
      endpoint: 'http://' + this.hostPort,
      sslEnabled: false,
      accessKeyId: '123',
      secretAccessKey: 'abc',
      s3ForcePathStyle: true
    })

    let counter = this.buckets.length
    for (const bucket of this.buckets) {
      s3.createBucket({
        Bucket: bucket
      }, onBucket)
    }

    function onBucket (err) {
      if (err) {
        return cb(err)
      }

      if (--counter === 0) {
        cb()
      }
    }
  }

  close (cb) {
    this.httpServer.close(cb)
  }
}

module.exports = FakeS3

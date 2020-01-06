'use strict'

const http = require('http')
const util = require('util')
const url = require('url')
const assert = require('assert')
const crypto = require('crypto')

class S3Object {
  constructor (bucket, key, content) {
    this.bucket = bucket
    this.key = key
    this.content = content

    const md5Hash = crypto.createHash('md5')
    md5Hash.update(content)
    this.md5 = md5Hash.digest('hex')

    // TODO: this.metadata
  }
}

class S3Bucket {
  constructor () {
    this._objects = new Map()
  }

  addObject (obj) {
    this._objects.set(obj.key, obj)
  }

  getObjects () {
    return new Array(...this._objects.values())
  }
}

class FakeS3 {
  constructor (options) {
    assert(options, 'options required')
    assert(options.prefix, 'options.prefix required')
    assert(options.buckets, 'options.buckets required')

    this.requestPort = 'port' in options ? options.port : 0
    this.requestHost = options.hostname || 'localhost'
    this.waitTimeout = options.waitTimeout || 5 * 1000

    this.httpServer = http.createServer()
    this.hostPort = null

    this.prefix = options.prefix
    this.buckets = options.buckets

    this.start = Date.now()

    this._buckets = new Map()
  }

  async bootstrap () {
    this.httpServer.on('request', (req, res) => {
      this._handleServerRequest(req, res)
    })

    await util.promisify((cb) => {
      this.httpServer.listen(this.requestPort, cb)
    })()

    this.hostPort = `localhost:${this.httpServer.address().port}`
    this.setupBuckets()
  }

  _handlePutObject (req, buf) {
    /* eslint-disable-next-line node/no-deprecated-api */
    const parsedUrl = url.parse(req.url, true)
    const parts = parsedUrl.pathname.split('/')
    if (parts.length < 3 || parts[0] !== '') {
      throw new Error('invalid url, expected /:bucket/:key')
    }

    const bucket = parts[1]
    const key = parts.slice(2, parts.length).join('/')

    if (req.headers['x-amz-copy-source']) {
      throw new Error('copyObject() not supported')
    }
    const query = parsedUrl.query
    if (query && query.uploadId) {
      throw new Error('putObjectMultipart not supported')
    }

    const s3bucket = this._buckets.get(bucket)
    if (!s3bucket) {
      const err = new Error('The specified bucket does not exist')
      err.code = 'NoSuchBucket'
      err.resource = bucket
      throw err
    }

    const obj = new S3Object(bucket, key, buf)
    s3bucket.addObject(obj)
    return obj
  }

  _handleListBuckets () {
    const buckets = [...this._buckets.keys()]
    let bucketsXML = ''

    const start = Math.floor(this.start / 1000)
    for (const b of buckets) {
      bucketsXML += `
        <Bucket>
          <CreationDate>${start}</CreationDate>
          <Name>${b}</Name>
        </Bucket>
      `
    }

    return `<ListBucketsOutput>
      <Buckets>
        ${bucketsXML}
      </Buckets>
      <Owner>
        <DisplayName>admin</DisplayName>
        <ID>1</ID>
      </Owner>
    </ListBucketsOutput>`
  }

  _handleGetObjectsV2 (req) {
    /* eslint-disable-next-line node/no-deprecated-api */
    const parsedUrl = url.parse(req.url, true)
    const parts = parsedUrl.pathname.split('/')
    if (parts.length > 2 || parts[0] !== '') {
      throw new Error('invalid url, expected /:bucket')
    }

    const bucket = parts[1]

    // TODO: handle parsedUrl.query.delimiter
    // TODO: handle parsedUrl.query.marker
    // TODO: handle parsedUrl.query.prefix
    // TODO: handle parsedUrl.query["max-keys"]

    const s3bucket = this._buckets.get(bucket)
    if (!s3bucket) {
      const err = new Error('The specified bucket does not exist')
      err.code = 'NoSuchBucket'
      err.resource = bucket
      throw err
    }

    const objects = s3bucket.getObjects()

    let contentXml = ''
    for (const o of objects) {
      contentXml += `<Contents>
        <Key>${o.key}</Key>
        <!-- TODO LastModified -->
        <ETag>${o.md5}</ETag>
        <Size>${o.content.length}</Size>
        <StorageClass>STANDARD</StorageClass>
        <!-- TODO OWNER -->
      </Contents>`
    }

    return `<ListBucketResult>
      <IsTruncated>false</IsTruncated>
      <Marker></Marker>
      <Name>${bucket}</Name>
      <Prefix></Prefix>
      <MaxKeys>1000</MaxKeys>
      <KeyCount>${objects.length}</KeyCount>
      <!-- TODO: support CommonPrefixes -->
      ${contentXml}
    </ListBucketResult>`
  }

  _buildError (err) {
    let resourceStr = ''
    if (err.resource) {
      resourceStr = `<Resource>${err.resource}</Resource>`
    }

    return `<Error>
      <Code>${err.code || 'InternalError'}</Code>
      <Message>${escapeXML(err.message)}</Message>
      ${resourceStr}
      <RequestId>1</RequestId>
    </Error>`
  }

  _writeError (err, res) {
    const xml = this._buildError(err)
    res.writeHead(500, { 'Content-Type': 'text/xml' })
    res.end(xml)
  }

  _handleServerRequest (req, res) {
    const buffers = []
    req.on('data', (chunk) => {
      buffers.push(chunk)
    })
    req.on('end', () => {
      const bodyBuf = Buffer.concat(buffers)

      if (req.method === 'PUT') {
        let obj
        try {
          obj = this._handlePutObject(req, bodyBuf)
        } catch (err) {
          return this._writeError(err, res)
        }

        res.setHeader('ETag', JSON.stringify(obj.md5))
        res.end()
      } else if (req.method === 'GET' && req.url === '/') {
        let xml
        try {
          xml = this._handleListBuckets(req, bodyBuf)
        } catch (err) {
          return this._writeError(err, res)
        }

        res.writeHead(200, { 'Content-Type': 'text/xml' })
        res.end(xml)
      } else if (req.method === 'GET') {
        let xml
        try {
          xml = this._handleGetObjectsV2(req, bodyBuf)
        } catch (err) {
          return this._writeError(err, res)
        }

        res.writeHead(200, { 'Content-Type': 'text/xml' })
        res.end(xml)
      } else {
        this._writeError(new Error(
          'url not supported: ' + req.method + ' ' + req.url
        ), res)
      }
    })
  }

  async getFiles (bucket) {
    const s3bucket = this._buckets.get(bucket)
    if (!s3bucket) {
      return {
        objects: []
      }
    }

    const objects = s3bucket.getObjects()
      .filter((o) => {
        return o.key.startsWith(this.prefix)
      })

    return { objects }
  }

  async waitForFiles (bucket, count) {
    const deadline = Date.now() + this.waitTimeout

    while (Date.now() <= deadline) {
      const info = await this.getFiles(bucket)
      if (info.objects.length === count) {
        return info
      }

      await sleep(100)
    }

    return null
  }

  setupBuckets () {
    for (const bucket of this.buckets) {
      this._buckets.set(bucket, new S3Bucket())
    }
  }

  async close () {
    await util.promisify((cb) => {
      this.httpServer.close(cb)
    })()
  }
}

module.exports = FakeS3

async function sleep (n) {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve()
    }, n)
  })
}

function escapeXML (str) {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/'/g, '&apos;')
    .replace(/"/g, '&quot;')
}

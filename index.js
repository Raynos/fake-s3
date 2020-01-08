'use strict'

const http = require('http')
const util = require('util')
const url = require('url')
const assert = require('assert')
const crypto = require('crypto')
const path = require('path')
const fs = require('fs')

const mkdirP = util.promisify(fs.mkdir)
const writeFileP = util.promisify(fs.writeFile)
const readFileP = util.promisify(fs.readFile)
const readdirP = util.promisify(fs.readdir)

class S3Object {
  constructor (bucket, key, content, md5, contentLength) {
    this.type = 's3-object'
    this.bucket = bucket
    this.key = key
    this.content = content
    this.md5 = md5
    this.contentLength = contentLength
    // TODO: this.metadata
  }
}

class CommonPrefix {
  constructor (prefix) {
    this.type = 's3-common-prefix'
    this.prefix = prefix
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
    assert('prefix' in options, 'options.prefix required')
    assert(
      options.buckets || options.cachePath,
      'options.buckets or options.cachePath required'
    )

    this.requestPort = 'port' in options ? options.port : 0
    this.requestHost = options.hostname || 'localhost'
    this.waitTimeout = options.waitTimeout || 5 * 1000

    this.touchedCache = false
    this.knownCaches = []

    this.httpServer = http.createServer()
    this.hostPort = null

    this.prefix = options.prefix
    this.initialBuckets = options.buckets || []
    this.cachePath = options.cachePath

    this.bucketsOwnerName = 'admin'
    this.bucketsOwnerID = '1'
    this.start = Date.now()
    this._buckets = new Map()

    this.tokens = {}
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

    if (this.cachePath) {
      await this.populateFromCache(this.cachePath)
    }
  }

  async tryMkdir (filePath) {
    try {
      await mkdirP(filePath)
    } catch (err) {
      if (err.code !== 'EEXIST') throw err
    }
  }

  /**
   * Can cache the output of `listBuckets()` directly.
   */
  async cacheBucketsToDisk (filePath, buckets) {
    this.touchedCache = true
    if (!this.knownCaches.includes(filePath)) {
      this.knownCaches.push(filePath)
    }

    await this.tryMkdir(filePath)
    await writeFileP(
      path.join(filePath, 'buckets.json'),
      JSON.stringify({
        type: 'cached-buckets',
        data: buckets
      }),
      'utf8'
    )
  }

  /**
   * Recommended to exhaustively get all objects and combine
   * them and then call this.
   */
  async cacheObjectsToDisk (filePath, bucketName, objects) {
    this.touchedCache = true
    if (!this.knownCaches.includes(filePath)) {
      this.knownCaches.push(filePath)
    }

    const key = encodeURIComponent(bucketName)
    await this.tryMkdir(filePath)
    await this.tryMkdir(path.join(filePath, 'buckets'))
    await this.tryMkdir(path.join(filePath, 'buckets', key))
    await writeFileP(
      path.join(filePath, 'buckets', key, 'objects.json'),
      JSON.stringify({
        type: 'cached-objects',
        bucketName,
        objects
      }),
      'utf8'
    )
  }

  async populateFromCache (filePath) {
    let bucketsStr
    try {
      bucketsStr = await readFileP(
        path.join(filePath, 'buckets.json'), 'utf8'
      )
    } catch (err) {
      if (err.code !== 'ENOENT') throw err
    }

    if (bucketsStr) {
      const buckets = JSON.parse(bucketsStr)
      this.populateBuckets(buckets.data)
    }

    let bucketDirs = null
    try {
      bucketDirs = await readdirP(path.join(filePath, 'buckets'))
    } catch (err) {
      if (err.code !== 'ENOENT') throw err
    }

    if (bucketDirs) {
      for (const bucketName of bucketDirs) {
        const objectsStr = await readFileP(path.join(
          filePath, 'buckets', bucketName, 'objects.json'
        ))
        const objectsInfo = JSON.parse(objectsStr)
        this.populateObjects(
          objectsInfo.bucketName, objectsInfo.objects
        )
      }
    }
  }

  populateBuckets (buckets) {
    this._buckets.clear()
    for (const b of buckets.Buckets) {
      this._buckets.set(b.Name, new S3Bucket())
    }

    this.bucketsOwnerID = buckets.Owner.ID
    this.bucketsOwnerName = buckets.Owner.DisplayName
  }

  populateObjects (bucketName, objects) {
    const bucket = this._buckets.get(bucketName)
    if (!bucket) throw new Error('invalid bucketName')

    for (const c of objects.Contents) {
      const obj = new S3Object(
        bucketName,
        c.Key,
        '',
        c.ETag,
        c.Size
      )
      bucket.addObject(obj)
    }
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

    const md5Hash = crypto.createHash('md5')
    md5Hash.update(buf)
    const md5 = md5Hash.digest('hex')
    const obj = new S3Object(bucket, key, buf, md5, buf.length)
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
        <DisplayName>${this.bucketsOwnerName}</DisplayName>
        <ID>${this.bucketsOwnerID}</ID>
      </Owner>
    </ListBucketsOutput>`
  }

  paginate (parsedUrl, rawObjects) {
    let maxKeys = 1000

    if (parsedUrl.query['max-keys']) {
      const queryMaxKeys = parseInt(parsedUrl.query['max-keys'], 10)
      if (queryMaxKeys < maxKeys) {
        maxKeys = queryMaxKeys
      }
    }

    let offset = 0
    const prevToken = parsedUrl.query['continuation-token']
    if (prevToken) {
      const tokenInfo = this.tokens[prevToken]
      delete this.tokens[prevToken]

      if (!tokenInfo) throw new Error('invalid next token')
      offset = tokenInfo.offset
    }

    const end = offset + maxKeys
    const resultObjects = rawObjects.slice(offset, end)
    const truncated = rawObjects.length > end

    let nextToken
    if (truncated) {
      nextToken = cuuid()
      this.tokens[nextToken] = { offset: end }
    }

    return {
      objects: resultObjects,
      prevToken: prevToken,
      maxKeys: maxKeys,
      nextToken: nextToken
    }
  }

  splitObjects (objects, delimiter, prefix) {
    const prefixSet = new Set()

    const out = []
    for (const obj of objects) {
      const key = prefix ? obj.key.slice(prefix.length) : obj.key

      const parts = key.split(delimiter)
      if (parts.length === 1) {
        out.push(obj)
      } else {
        const segment = parts[0] + delimiter
        if (prefixSet.has(segment)) {
          continue
        } else {
          out.push(new CommonPrefix((prefix || '') + segment))
          prefixSet.add(segment)
        }
      }
    }
    return out
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

    const s3bucket = this._buckets.get(bucket)
    if (!s3bucket) {
      const err = new Error('The specified bucket does not exist')
      err.code = 'NoSuchBucket'
      err.resource = bucket
      throw err
    }

    let objects = s3bucket.getObjects()
    objects.sort((a, b) => {
      return a.key < b.key ? -1 : 1
    })

    const startAfter = parsedUrl.query['start-after']
    if (startAfter) {
      const index = objects.findIndex((o) => {
        return o.key === startAfter
      })
      if (index >= 0) {
        objects = objects.slice(index + 1)
      }
    }

    const prefix = parsedUrl.query.prefix
    if (prefix) {
      objects = objects.filter((o) => {
        return o.key.startsWith(prefix)
      })
    }

    const delimiter = parsedUrl.query.delimiter
    if (delimiter) {
      objects = this.splitObjects(objects, delimiter, prefix)
    }

    const {
      prevToken, nextToken, maxKeys,
      objects: resultObjects
    } = this.paginate(parsedUrl, objects)

    let contentXml = ''
    let commonPrefixes = ''
    for (const o of resultObjects) {
      if (o.type === 's3-object') {
        contentXml += `<Contents>
          <Key>${o.key}</Key>
          <!-- TODO LastModified -->
          <ETag>${o.md5}</ETag>
          <Size>${o.contentLength}</Size>
          <StorageClass>STANDARD</StorageClass>
          <!-- TODO OWNER -->
        </Contents>`
      } else if (o.type === 's3-common-prefix') {
        commonPrefixes += `<CommonPrefixes>
          <Prefix>${o.prefix}</Prefix>
        </CommonPrefixes>`
      }
    }

    const truncated = Boolean(nextToken)
    const contToken = nextToken
      ? '<NextContinuationToken>' + nextToken +
        '</NextContinuationToken>'
      : ''
    const prevContToken = prevToken
      ? '<ContinuationToken>' + prevToken +
        '</ContinuationToken>'
      : ''
    const delimiterResp = delimiter
      ? '<Delimiter>' + delimiter + '</Delimiter>'
      : ''

    return `<ListObjectsV2Output>
      <IsTruncated>${truncated}</IsTruncated>
      <Marker></Marker>
      <Name>${bucket}</Name>
      <Prefix>${prefix || ''}</Prefix>
      <MaxKeys>${maxKeys}</MaxKeys>
      <KeyCount>${resultObjects.length}</KeyCount>
      <!-- TODO: support CommonPrefixes -->
      ${contentXml}
      ${commonPrefixes}
      ${contToken}
      ${prevContToken}
      ${delimiterResp}
    </ListObjectsV2Output>`
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
    for (const bucket of this.initialBuckets) {
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

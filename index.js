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

const stripCreds = /Credential=([\w-/0-9a-zA-Z]+),/

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

    this.start = Date.now()

    this._profiles = new Map()
    this._bucketOwnerInfo = new Map()

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
  async cacheBucketsToDisk (filePath, accessKeyId, buckets) {
    this.touchedCache = true
    if (!this.knownCaches.includes(filePath)) {
      this.knownCaches.push(filePath)
    }

    await this.tryMkdir(filePath)
    await this.tryMkdir(path.join(filePath, 'buckets'))
    await writeFileP(
      path.join(filePath, 'buckets', `${accessKeyId}.json`),
      JSON.stringify({
        type: 'cached-buckets',
        data: buckets,
        accessKeyId: accessKeyId
      }),
      'utf8'
    )
  }

  /**
   * Recommended to exhaustively get all objects and combine
   * them and then call this.
   */
  async cacheObjectsToDisk (filePath, accessKeyId, bucketName, objects) {
    this.touchedCache = true
    if (!this.knownCaches.includes(filePath)) {
      this.knownCaches.push(filePath)
    }

    const key = encodeURIComponent(bucketName)
    await this.tryMkdir(filePath)
    await this.tryMkdir(path.join(filePath, 'objects'))
    await this.tryMkdir(path.join(filePath, 'objects', key))
    await writeFileP(
      path.join(filePath, 'objects', key, `${accessKeyId}.json`),
      JSON.stringify({
        type: 'cached-objects',
        bucketName,
        accessKeyId,
        objects
      }),
      'utf8'
    )
  }

  async populateFromCache (filePath) {
    let bucketFiles = null
    try {
      bucketFiles = await readdirP(path.join(filePath, 'buckets'))
    } catch (err) {
      if (err.code !== 'ENOENT') throw err
    }

    if (bucketFiles) {
      for (const fileName of bucketFiles) {
        const bucketStr = await readFileP(path.join(
          filePath, 'buckets', fileName
        ))
        const buckets = JSON.parse(bucketStr)
        this.populateBuckets(buckets.accessKeyId, buckets.data)
      }
    }

    // TODO: fix me

    let objectDirs = null
    try {
      objectDirs = await readdirP(path.join(filePath, 'objects'))
    } catch (err) {
      if (err.code !== 'ENOENT') throw err
    }

    if (!objectDirs) {
      return
    }

    for (const bucketName of objectDirs) {
      const objectFiles = await readdirP(path.join(
        filePath, 'objects', bucketName
      ))

      for (const objectFile of objectFiles) {
        const objectsStr = await readFileP(path.join(
          filePath, 'objects', bucketName, objectFile
        ))
        const objectsInfo = JSON.parse(objectsStr)
        this.populateObjects(
          objectsInfo.accessKeyId,
          objectsInfo.bucketName,
          objectsInfo.objects
        )
      }
    }
  }

  populateBuckets (accessKeyId, buckets) {
    if (!this._profiles.has(accessKeyId)) {
      this._profiles.set(accessKeyId, new Map())
    }

    const bucketsMap = this._profiles.get(accessKeyId)
    for (const b of buckets.Buckets) {
      bucketsMap.set(b.Name, new S3Bucket())
      this._bucketOwnerInfo.set(b.Name, buckets.Owner)
    }
  }

  populateObjects (accessKeyId, bucketName, objects) {
    const bucketMap = this._profiles.get(accessKeyId)
    if (!bucketMap) throw new Error('invalid accessKeyId')

    const bucket = bucketMap.get(bucketName)
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

  _findBucket (bucket) {
    for (const map of this._profiles.values()) {
      if (map.has(bucket)) return map.get(bucket)
    }
    return null
  }

  async getFiles (bucket) {
    const s3bucket = this._findBucket(bucket)
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
    if (this.initialBuckets.length === 0) {
      return
    }

    const bucketsMap = new Map()
    this._profiles.set('default', bucketsMap)

    for (const bucket of this.initialBuckets) {
      bucketsMap.set(bucket, new S3Bucket())
    }
  }

  async close () {
    await util.promisify((cb) => {
      this.httpServer.close(cb)
    })()
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

    // For the upload use case we always write into the default
    // profile and not into the profiles hydrated from cache.
    const bucketsMap = this._profiles.get('default')
    const s3bucket = bucketsMap ? bucketsMap.get(bucket) : null
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

  _getBucketsMap (req) {
    const authHeader = req.headers['authorization']
    const match = authHeader.match(stripCreds)
    let profile = 'default'
    if (match) {
      const creds = match[0].slice(11)
      const accessKeyId = creds.split('/')[0]
      profile = accessKeyId
    }

    if (this._profiles.has(profile)) {
      return this._profiles.get(profile)
    }

    return this._profiles.get('default')
  }

  _handleListBuckets (req) {
    const bucketsMap = this._getBucketsMap(req)
    const buckets = bucketsMap ? [...bucketsMap.keys()] : []

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

    const ownerInfo = this._bucketOwnerInfo.get(buckets[0])
    const displayName = ownerInfo ? ownerInfo.DisplayName : 'admin'
    const id = ownerInfo ? ownerInfo.ID : '1'

    return `<ListBucketsOutput>
      <Buckets>
        ${bucketsXML}
      </Buckets>
      <Owner>
        <DisplayName>${displayName}</DisplayName>
        <ID>${id}</ID>
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
    const bucketsMap = this._getBucketsMap(req)
    const s3bucket = bucketsMap ? bucketsMap.get(bucket) : null
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

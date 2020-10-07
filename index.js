// @ts-check
'use strict'

const http = require('http')
const util = require('util')
const url = require('url')
/** @type {import('assert')} */
const assert = require('assert')
const crypto = require('crypto')
const path = require('path')
const fs = require('fs')

const mkdirP = util.promisify(fs.mkdir)
const writeFileP = util.promisify(fs.writeFile)
const readFileP = util.promisify(fs.readFile)
const readdirP = util.promisify(fs.readdir)

const stripCreds = /Credential=([\w-/0-9a-zA-Z]+),/

/**
 * @typedef {(err?: Error) => void} Callback
 * @typedef {{
 *    message: string,
 *    code?: string,
 *    resource?: string
 * }} S3Error
 * @typedef {{ DisplayName: string, ID: string }} BucketOwner
 * @typedef {{ Name: string, CreationDate: Date }} S3BucketItem
 * @typedef {{
 *    Key: string,
 *    LastModified: Date,
 *    ETag: string,
 *    Size: number,
 *    StorageClass: string
 * }} ObjectContent
 */

class NoSuchBucketError extends Error {
  /**
   * @param {string} message
   * @param {string} bucket
   */
  constructor (message, bucket) {
    super(message)

    this.code = 'NoSuchBucket'
    this.resource = bucket
  }
}

class S3Object {
  /**
   * @param {string} bucket
   * @param {string} key
   * @param {string | Buffer} content
   * @param {string} lastModified
   * @param {string} md5
   * @param {number} contentLength
   */
  constructor (bucket, key, content, lastModified, md5, contentLength) {
    /** @type {"s3-object"} */
    this.type = 's3-object'
    this.bucket = bucket
    this.key = key
    this.content = content
    this.lastModified = lastModified
    this.md5 = md5
    this.contentLength = contentLength
    // TODO: this.metadata
  }
}

class CommonPrefix {
  /** @param {string} prefix */
  constructor (prefix) {
    /** @type {"s3-common-prefix"} */
    this.type = 's3-common-prefix'
    this.prefix = prefix
  }
}

class S3Bucket {
  constructor () {
    /**
     * @type {Map<string, S3Object>}
     */
    this._objects = new Map()
  }

  /** @param {S3Object} obj */
  addObject (obj) {
    this._objects.set(obj.key, obj)
  }

  getObjects () {
    return [...this._objects.values()]
  }
}

class FakeS3 {
  /**
   * @param {{
   *    prefix: string,
   *    buckets?: string[],
   *    cachePath?: string,
   *    hostname?: string,
   *    port?: number,
   *    waitTimeout?: number
   * }} options
   */
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
    /** @type {string[]} */
    this.knownCaches = []

    this.httpServer = http.createServer()
    this.hostPort = null

    this.prefix = options.prefix
    this.initialBuckets = options.buckets || []
    this.cachePath = options.cachePath

    this.start = Date.now()

    /**
     * @type {Map<string, Map<string, S3Bucket>>}
     */
    this._profiles = new Map()
    /**
     * @type {Map<string, BucketOwner>}
     */
    this._bucketOwnerInfo = new Map()

    /** @type {Record<string, {
     *      offset: number,
     *      startAfter?: string
     *  }>}
     */
    this.tokens = {}
  }

  async bootstrap () {
    this.httpServer.on('request', (req, res) => {
      this._handleServerRequest(req, res)
    })

    await util.promisify((
      /** @type {Callback} */ cb
    ) => {
      this.httpServer.listen(this.requestPort, cb)
    })()

    const addr = this.httpServer.address()
    const port = (addr && typeof addr === 'object')
      ? addr.port : -1
    this.hostPort = `localhost:${port}`
    this.setupBuckets()

    if (this.cachePath) {
      await this.populateFromCache(this.cachePath)
    }
  }

  getHostPort () {
    if (!this.hostPort) return ''
    return this.hostPort
  }

  /** @param {string} filePath */
  async tryMkdir (filePath) {
    try {
      await mkdirP(filePath)
    } catch (err) {
      if (err.code !== 'EEXIST') throw err
    }
  }

  /**
   * Can cache the output of `listBuckets()` directly.
   * @param {string} filePath
   * @param {string} accessKeyId
   * @param {{
   *    Buckets?: Partial<S3BucketItem>[],
   *    Owner?: Partial<BucketOwner>
   * }} buckets
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
   * @param {string} filePath
   * @param {string} accessKeyId
   * @param {string} bucketName
   * @param {{
   *    Contents?: Partial<ObjectContent>[]
   * }} objects
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

  /**
   * @param {string} filePath
   */
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
        ), 'utf8')
        const buckets = JSON.parse(bucketStr)
        this.populateBuckets(buckets.accessKeyId, buckets.data)
      }
    }

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
        ), 'utf8')
        const objectsInfo = JSON.parse(objectsStr)
        this.populateObjects(
          objectsInfo.accessKeyId,
          objectsInfo.bucketName,
          objectsInfo.objects
        )
      }
    }
  }

  /**
   * @param {string} accessKeyId
   * @param {{
   *    Owner: BucketOwner,
   *    Buckets: Array<{
   *        Name: string
   *    }>
   * }} buckets
   */
  populateBuckets (accessKeyId, buckets) {
    let bucketsMap = this._profiles.get(accessKeyId)
    if (!bucketsMap) {
      bucketsMap = new Map()
      this._profiles.set(accessKeyId, bucketsMap)
    }
    for (const b of buckets.Buckets) {
      bucketsMap.set(b.Name, new S3Bucket())
      this._bucketOwnerInfo.set(b.Name, buckets.Owner)
    }
  }

  /**
   * @param {string} accessKeyId
   * @param {string} bucketName
   * @param {{
   *    Contents: Array<{
   *        Key: string,
   *        LastModified: string,
   *        ETag: string,
   *        Size: number
   *    }>
   * }} objects
   */
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
        c.LastModified,
        c.ETag,
        c.Size
      )
      bucket.addObject(obj)
    }
  }

  /**
   * @param {string} bucketName
   * @returns {S3Bucket | null}
   */
  _findBucket (bucketName) {
    for (const map of this._profiles.values()) {
      const bucket = map.get(bucketName)
      if (bucket) return bucket
    }
    return null
  }

  /**
   * @param {string} bucket
   * @returns {Promise<{ objects: S3Object[] }>}
   */
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

  /**
   * @param {string} bucket
   * @param {number} count
   */
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
    await util.promisify((
      /** @type {Callback} */ cb
    ) => {
      this.httpServer.close(cb)
    })()
  }

  /**
   *
   * @param {import('http').IncomingMessage} req
   * @param {Buffer} buf
   */
  _handlePutObject (req, buf) {
    const reqUrl = req.url || ''

    /* eslint-disable-next-line node/no-deprecated-api */
    const parsedUrl = url.parse(reqUrl, true)
    const parts = (parsedUrl.pathname || '').split('/')
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
      const err = new NoSuchBucketError(
        'The specified bucket does not exist', bucket
      )
      throw err
    }

    const md5Hash = crypto.createHash('md5')
    md5Hash.update(buf)
    const md5 = md5Hash.digest('hex')
    const lastModified = new Date().toISOString()
    const obj = new S3Object(
      bucket, key, buf, lastModified, md5, buf.length
    )
    s3bucket.addObject(obj)
    return obj
  }

  /**
   * @param {import('http').IncomingMessage} req
   */
  _getBucketsMap (req) {
    const authHeader = req.headers['authorization'] || ''
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

  /**
   * @param {import('http').IncomingMessage} req
   */
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

  /**
   * @param {import('url').UrlWithParsedQuery} parsedUrl
   * @param {(S3Object | CommonPrefix)[]} rawObjects
   * @returns {{
   *      objects: (S3Object | CommonPrefix)[]
   *      prevToken: string | undefined,
   *      maxKeys: number,
   *      nextToken: string | undefined
   * }}
   */
  paginate (parsedUrl, rawObjects) {
    let maxKeys = 1000

    if (parsedUrl.query['max-keys']) {
      let maxKeysStr = parsedUrl.query['max-keys']
      if (Array.isArray(maxKeysStr)) maxKeysStr = maxKeysStr[0]

      const queryMaxKeys = parseInt(maxKeysStr, 10)
      if (queryMaxKeys < maxKeys) {
        maxKeys = queryMaxKeys
      }
    }

    let offset = 0
    let startAfter = parsedUrl.query['start-after']
    if (Array.isArray(startAfter)) startAfter = startAfter[0]
    let prevToken = parsedUrl.query['continuation-token']
    if (Array.isArray(prevToken)) prevToken = prevToken[0]
    if (prevToken) {
      const tokenInfo = this.tokens[prevToken]
      delete this.tokens[prevToken]

      if (!tokenInfo) throw new Error('invalid next token')
      offset = tokenInfo.offset

      if (tokenInfo.startAfter) {
        startAfter = tokenInfo.startAfter
      }
    }

    if (startAfter) {
      const index = rawObjects.findIndex((o) => {
        if (o.type === 's3-common-prefix') return
        return o.key === startAfter
      })
      if (index >= 0) {
        rawObjects = rawObjects.slice(index + 1)
      }
    }

    const end = offset + maxKeys
    const resultObjects = rawObjects.slice(offset, end)
    const truncated = rawObjects.length > end

    let nextToken
    if (truncated) {
      nextToken = cuuid()
      this.tokens[nextToken] = {
        offset: end,
        startAfter: startAfter
      }
    }

    return {
      objects: resultObjects,
      prevToken: prevToken,
      maxKeys: maxKeys,
      nextToken: nextToken
    }
  }

  /**
   * @param {S3Object[]} objects
   * @param {string} delimiter
   * @param {string} [prefix]
   * @returns {(S3Object | CommonPrefix)[]}
   */
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

  /**
   *
   * @param {import('http').IncomingMessage} req
   */
  _handleGetObjectsV2 (req) {
    /* eslint-disable-next-line node/no-deprecated-api */
    const parsedUrl = url.parse(req.url || '', true)
    const parts = (parsedUrl.pathname || '').split('/')
    if (parts.length > 2 || parts[0] !== '') {
      throw new Error('invalid url, expected /:bucket')
    }

    const bucket = parts[1]
    const bucketsMap = this._getBucketsMap(req)
    const s3bucket = bucketsMap ? bucketsMap.get(bucket) : null
    if (!s3bucket) {
      const err = new NoSuchBucketError(
        'The specified bucket does not exist', bucket
      )
      throw err
    }

    let objects = s3bucket.getObjects()
    objects.sort((a, b) => {
      return a.key < b.key ? -1 : 1
    })

    let prefix = parsedUrl.query.prefix
    if (Array.isArray(prefix)) prefix = prefix[0]
    if (prefix) {
      const filterPrefix = prefix
      objects = objects.filter((o) => {
        return o.key.startsWith(filterPrefix)
      })
    }

    let delimiter = parsedUrl.query.delimiter
    /** @type {(S3Object | CommonPrefix)[]} */
    let allObjects
    if (delimiter) {
      if (Array.isArray(delimiter)) delimiter = delimiter[0]
      allObjects = this.splitObjects(objects, delimiter, prefix)
    } else {
      allObjects = objects
    }

    const {
      prevToken, nextToken, maxKeys,
      objects: resultObjects
    } = this.paginate(parsedUrl, allObjects)

    let contentXml = ''
    let commonPrefixes = ''
    for (const o of resultObjects) {
      if (o.type === 's3-object') {
        contentXml += `<Contents>
          <Key>${o.key}</Key>
          <LastModified>${o.lastModified}</LastModified>
          <ETag>${o.md5}</ETag>
          <Size>${o.contentLength}</Size>
          <StorageClass>STANDARD</StorageClass>
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

  /**
   * @param {S3Error} err
   */
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

  /**
   * @param {S3Error} err
   * @param {import('http').ServerResponse} res
   */
  _writeError (err, res) {
    const xml = this._buildError(err)
    res.writeHead(500, { 'Content-Type': 'text/xml' })
    res.end(xml)
  }

  /**
   * @param {import('http').IncomingMessage} req
   * @param {import('http').ServerResponse} res
   */
  _handleServerRequest (req, res) {
    /** @type {Buffer[]} */
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
          xml = this._handleListBuckets(req)
        } catch (err) {
          return this._writeError(err, res)
        }

        res.writeHead(200, { 'Content-Type': 'text/xml' })
        res.end(xml)
      } else if (req.method === 'GET') {
        let xml
        try {
          xml = this._handleGetObjectsV2(req)
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

/**
 *
 * @param {number} n
 */
async function sleep (n) {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve()
    }, n)
  })
}

/**
 *
 * @param {string} str
 */
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

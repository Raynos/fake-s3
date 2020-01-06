# fake-s3

<!--
    [![build status][build-png]][build]
    [![Coverage Status][cover-png]][cover]
    [![Davis Dependency status][dep-png]][dep]
-->

<!-- [![NPM][npm-png]][npm] -->

a fake s3 server for testing purposes.

This is a zero dependency implementation that stores all objects
in memory

## Example

```js
const FakeS3 = require('fake-s3');
const AWS = require('aws-sdk')

const server = new FakeS3({
  buckets: ['my-bucket'],
  prefix: 'files-i-care-about/'
})

// starts the server on specified port
await server.bootstrap()

// this field now exists and contains the actual hostPort
server.hostPort

// Create an S3 client connected to it
const s3 = new AWS.S3({
  endpoint: `http://${server.hostPort}`
  sslEnabled: false,
  accessKeyId: '123',
  secretAccessKey: 'abc',
  s3ForcePathStyle: true
})

// can wait for files
const files = await server.waitForFiles('my-bucket', 2)
// will yield you back when two files have been uploaded

// shutdown server
await server.close()
```

## Support

The following `aws-sdk` methods are supported

 - `s3.listBuckets()`
 - `s3.listObjectsV2()`
 - `s3.upload()`

## Docs

### `var server = new FakeS3(options)`

 - `options.prefix` : prefix for `getFiles()` and `waitForFiles()` ;
      necessary to support multi part uploads, otherwise
      `waitForFiles()` will return too early when N parts have
      been uploaded.
 - `options.buckets` : an array of buckets to create.

### `server.hostPort`

This is the `hostPort` that the server is listening on, this
will be non-null after `bootstrap()` finishes.

### `await server.bootstrap()`

starts the server

### `await getFiles(bucket)`

gets all files in a bucket

### `await waitForFiles(bucket, count)`

this will wait for file uploads to finish and calls `getFiles()`
and returns them once it's finished.

This is useful if your application does background uploads and you
want to be notified when they are finished.

### `await server.close()`

closes the HTTP server.

## Installation

`npm install fake-s3`

## Tests

`npm test`

## Contributors

 - Raynos

## MIT Licensed

  [build-png]: https://secure.travis-ci.org/Raynos/fake-s3.png
  [build]: https://travis-ci.org/Raynos/fake-s3
  [cover-png]: https://coveralls.io/repos/Raynos/fake-s3/badge.png
  [cover]: https://coveralls.io/r/Raynos/fake-s3
  [dep-png]: https://david-dm.org/Raynos/fake-s3.png
  [dep]: https://david-dm.org/Raynos/fake-s3
  [npm-png]: https://nodei.co/npm/fake-s3.png?stars&downloads
  [npm]: https://nodei.co/npm/fake-s3

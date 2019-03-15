# fake-s3

<!--
    [![build status][build-png]][build]
    [![Coverage Status][cover-png]][cover]
    [![Davis Dependency status][dep-png]][dep]
-->

<!-- [![NPM][npm-png]][npm] -->

a fake s3 server for testing purposes.

This module uses `s3rver` which does most of the heavy lifting.

The actual files uploaded to S3 are persisted to disk in
`os.tmpdir()`

## Example

```js
var FakeS3 = require("fake-s3");

var server = new FakeS3({
  port: 0,
  buckets: ['my-bucket'],
  prefix: 'files-i-care-about/'
})

// starts the server on specified port
server.bootstrap((err) => {
  // handle err

  // this field now exists and contains the actual hostPort
  server.hostPort
})

// can wait for files
server.waitForFiles('my-bucket', 2, (err, files) => {
  // will call you back when two files have been uploaded
})

// shutdown server
server.close()
```

## Docs

### `var server = new FakeS3(options)`

 - `options.port` : the port to lsiten on, defaults to `0`
 - `options.hostname` : host to listen on, defaults to `localhost`
 - `options.silent` : passed through to `s3rver`, defaults to `true`
 - `options.prefix` : prefix for `getFiles()` and `waitForFiles()` ;
      necessary to support multi part uploads, otherwise
      `waitForFiles()` will return too early when N parts have
      been uploaded.
 - `options.buckets` : an array of buckets to create.

### `server.hostPort`

This is the `hostPort` that the server is listening on, this
will be non-null after `bootstrap()` finishes.

### `server.bootstrap(cb)`

starts the server

### `getFiles(bucket, cb)`

gets all files in a bucket

### `waitForFiles(bucket, count, cb)`

this will wait for file uploads to finish and calls `getFiles()`
and returns them once it's finished.

This is useful if your application does background uploads and you
want to be notified when they are finished.

### `server.close()`

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

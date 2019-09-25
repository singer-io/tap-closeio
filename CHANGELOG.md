# Changelog

## 1.6.1
  * Change the schema of the `activities` stream because only `activities`
    of `_type` "Meeting" returns anything the `users` field. And when it
    does, we see strings and not `user` objects.
    [#19](https://github.com/singer-io/tap-closeio/pull/19)

## 1.6.0
  * Update stream selection to use metadata rather than deprecated annotated-schema [#18](https://github.com/singer-io/tap-closeio/pull/18)

## 1.5.3
  * Increases the timeout on HTTP requests for data [#17](https://github.com/singer-io/tap-closeio/pull/17)

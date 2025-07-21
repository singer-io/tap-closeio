# Changelog

## 1.6.5
  * Bump dependency versions for twistlock compliance [#42](https://github.com/singer-io/tap-closeio/pull/42)

## 1.6.4
  * Ensures future-dated bookmarks cannot be written to state. Adds corresponding unittests [#30](https://github.com/singer-io/tap-closeio/pull/30)

## 1.6.3
  * Update pagination to respect max page limits [#27](https://github.com/singer-io/tap-closeio/pull/27)

## 1.6.2
  * Increases the timeout on HTTP requests for data [#21](https://github.com/singer-io/tap-closeio/pull/21)

## 1.6.1
  * Change the schema of the `activities` stream because only `activities`
    of `_type` "Meeting" returns anything the `users` field. And when it
    does, we see strings and not `user` objects.
    [#19](https://github.com/singer-io/tap-closeio/pull/19)

## 1.6.0
  * Update stream selection to use metadata rather than deprecated annotated-schema [#18](https://github.com/singer-io/tap-closeio/pull/18)

## 1.5.3
  * Increases the timeout on HTTP requests for data [#17](https://github.com/singer-io/tap-closeio/pull/17)

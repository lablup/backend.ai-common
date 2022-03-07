Changes
=======

<!--
    You should *NOT* be adding new change log entries to this file, this
    file is managed by towncrier. You *may* edit previous change logs to
    fix problems like typo corrections or such.

    To add a new change log entry, please refer
    https://pip.pypa.io/en/latest/development/contributing/#news-entries

    We named the news folder "changes".

    WARNING: Don't drop the last line!
-->

<!-- towncrier release notes start -->

## 21.09.7 (2022-03-07)

### Features
* Add `architecture` variable on `ImageRef` class to support multi-architecture image. ([#118](https://github.com/lablup/backend.ai-common/issues/118))
* Add architecture name alias mapping to convert `manager.machine()` output as docker API's norm. ([#119](https://github.com/lablup/backend.ai-common/issues/119))

### Fixes
* Silence a typing error in the `config` module due to `toml.decoder`'s way of tagging dict objects at runtime to distinguish inline and non-inline tables ([#117](https://github.com/lablup/backend.ai-common/issues/117))

### Miscellaneous
* Update pip caching in GitHub Actions to use the scheme managed by actions/setup-python ([#116](https://github.com/lablup/backend.ai-common/issues/116))


## 21.09.6 (2022-01-26)

### Fixes
* Improve the retry condition check for Redis command execution failures when used with pipelines ([#115](https://github.com/lablup/backend.ai-common/issues/115))
* Improve stability of `EventDispatcher`'s pub/sub messaging pattern using proper `last_id` and `XTRIM` command as a stream garbage collector ([#114](https://github.com/lablup/backend.ai-common/issues/114))


## 21.09.5 (2022-01-13)

### Fixes
* Improve `EventDispatcher` stability by earlier `XACK` and explicit `XDEL` Redis stream commands ([#110](https://github.com/lablup/backend.ai-common/issues/110))
* Adopt `aiotools.PersistentTaskGroup` to manager event handler tasks in `EventDispatcher` to capture unhandled exceptions explicitly and reduce boilerplate codes ([#111](https://github.com/lablup/backend.ai-common/issues/111))
* Import and update `EventDispatcher` test cases from the manager sources with a minor refactoring to add new optional constructor arguments for custom exception handlers ([#112](https://github.com/lablup/backend.ai-common/issues/112))


## 21.09.4 (2022-01-10)

### Fixes
* Use a fixed value for the Redis stream consumer ID in `EventDispatcher` to avoid accumulation of unused consumers upon service restarts, which causes database transaction flooding and slow-down of event processing ([#109](https://github.com/lablup/backend.ai-common/issues/109))


## 21.09.3 (2022-01-10)

### Fixes
* Fix minor typing errors discovered by mypy 0.920 update ([#104](https://github.com/lablup/backend.ai-common/issues/104))
* Update mypy to 0.930 and fix newly discovered type errors ([#105](https://github.com/lablup/backend.ai-common/issues/105))
* Fix potential deadlock upon shutdown of service daemons ([#106](https://github.com/lablup/backend.ai-common/issues/106))
* Fix typing issues of `StringSetFlag` by refactoring it using a separate interface definition file ([#107](https://github.com/lablup/backend.ai-common/issues/107))
* fix file logging crashing when backup-count option not set ([#108](https://github.com/lablup/backend.ai-common/issues/108))


## 21.09.2 (2021-12-15)

### Features
* Migrate file-based lock code from storage-proxy package ([#98](https://github.com/lablup/backend.ai-common/issues/98))
* Update `validators.TimeDuration` class to support years and months ([#99](https://github.com/lablup/backend.ai-common/issues/99))

### Fixes
* Remove an unreachable statement in `BinarySize.__format__()` ([#100](https://github.com/lablup/backend.ai-common/issues/100))


## 21.09.1 (2021-11-11)

### Fixes
* Implement test cases for UID/GID trafaret extensions ([#90](https://github.com/lablup/backend.ai-common/issues/90))
* Upgrade aiohttp from 3.7 to 3.8 series ([#95](https://github.com/lablup/backend.ai-common/issues/95))
* Adjust the default Redis connection keepalive options to be calculated similarly to how Redis internally does ([#96](https://github.com/lablup/backend.ai-common/issues/96))
* Update `async_timeout` API usage ([#97](https://github.com/lablup/backend.ai-common/issues/97))


## 21.09.0 (2021-11-08)

### Features
* Add `KernelPullProgressEvent` to represent incremental progress of image pulling when creating new kernels ([#87](https://github.com/lablup/backend.ai-common/issues/87))
* Upgrade to aioredis v2 with a major rewrite of the event bus using Redis streams ([#88](https://github.com/lablup/backend.ai-common/issues/88))

### Fixes
* Improve stability of test cases using a single Redis container as fixture (a follow-up to #88) ([#91](https://github.com/lablup/backend.ai-common/issues/91))
* Prevent "set size changed" error while iterating over event handlers, which may happen on startup on managers ([#92](https://github.com/lablup/backend.ai-common/issues/92))
* Explicitly shutdown the thread pool executor used for etcd API invocation ([#94](https://github.com/lablup/backend.ai-common/issues/94))

### Miscellaneous
* Add more test cases for parsing host-port pairs ([#89](https://github.com/lablup/backend.ai-common/issues/89))


## 21.09.0a2 (2021-09-28)

### Features
* Add support for kubernetes mount types ([eda7a5fa652](https://github.com/lablup/backend.ai-common/commit/eda7a5fa652bdfd07f9478cc75a1dc40a5637be7))


## 21.09.0a1 (2021-08-25)

### Features
* Update for the manager scheduler v3 (lablup/backend.ai-manager#415)
  - Fix LogSeverity serialization error for AgentErrorEvent
  - Add new events for the updated manager scheduler ([#76](https://github.com/lablup/backend.ai-common/issues/76))
* Implement coalescing of event handler invocation when there are bursts of the same events within a short period of time.
  Also improve stability of event dispatchers by inserting explicit context switches between different events. ([#77](https://github.com/lablup/backend.ai-common/issues/77))
* Add the `sd_notify` module to provide detailed status information when run via systemd ([#84](https://github.com/lablup/backend.ai-common/issues/84))

### Fixes
* Refine the stability update by recategorizing `KernelCancelledEvent` as a creation event instead of a termination event ([#70](https://github.com/lablup/backend.ai-common/issues/70))
* Allow overriding of msgpack wrapper method's keyword argument options. This should have been possible but it was not due to duplicate kwargs errors. ([#79](https://github.com/lablup/backend.ai-common/issues/79))
* Update for batch-type session refactor/fix, adding `cancel_tasks()` utility function and refactoring the utils module ([#80](https://github.com/lablup/backend.ai-common/issues/80))
* Remove the discouraged `loop` argument from the `AsyncFileWriter` constructor ([#81](https://github.com/lablup/backend.ai-common/issues/81))
* Unlink the logger socket only if the socket file exists ([#85](https://github.com/lablup/backend.ai-common/issues/85))

### Miscellaneous
* Update package dependencies ([#83](https://github.com/lablup/backend.ai-common/issues/83))


## Older changelogs

* [21.03](https://github.com/lablup/backend.ai-common/blob/21.03/CHANGELOG.md)
* [20.09](https://github.com/lablup/backend.ai-common/blob/20.09/CHANGELOG.md)
* [20.03](https://github.com/lablup/backend.ai-common/blob/20.03/CHANGELOG.md)


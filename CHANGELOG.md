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

.. towncrier release notes start

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


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

20.03.2 (2020-08-27)
--------------------

### Features
* Add `validators.PurePath`, a simpelified and non-syscall-involving version of `validators.Path` ([#44](https://github.com/lablup/backend.ai-common/issues/44))
* Implement an ultimate exception pickler to log arbitrary errors, `PickledException`, which carries the `repr()`-ed original exception ([#45](https://github.com/lablup/backend.ai-common/issues/45))


20.03.1 (2020-07-29)
--------------------

* Hotfix for a packaging issue, missing `common.plugin` subpackage in the distributions.

20.03.0 (2020-07-28)
--------------------

* No changes since RC1

20.03.0rc1 (2020-07-23)
-----------------------

### Features
* Add option to hook plugin dispatcher to explicitly reject if there are no hook plugins that provides handlers for the given hook event ([#43](https://github.com/lablup/backend.ai-common/issues/43))

### Fixes
* Fix type-safety of users of `types.DeviceModelInfo` with device-specific data ([#41](https://github.com/lablup/backend.ai-common/issues/41))
* Fix handling of already-parsed IP addr objs when validating HostPortPair inputs ([#42](https://github.com/lablup/backend.ai-common/issues/42))


20.03.0b6 (2020-07-02)
----------------------

### Breaking Changes
* Revamped the plugin API and subsystem. ([#36](https://github.com/lablup/backend.ai-common/issues/36))
  - All plugins now must subclass the `common.plugin.AbstractPlugin` interface.
  - It is highly recommended to subclass the `common.plugin.BasePluginContext` for specific plugin groups, as it now provides automatic etcd configuration update propagation to individual plugins and the lifecycle management.
  - Hook plugins now have explicit and clear semantics for event callbacks such as acceptance via ALL_COMPLETED/FIRST_COMPLETED requirements and one-way notifications, via `HookPluginContext`.

### Features
* Add function to convert humanized timedelta string to timedelta obj. ([#37](https://github.com/lablup/backend.ai-common/issues/37))

### Miscellaneous
* Change SessionId's base type from str to uuid.UUID. ([#38](https://github.com/lablup/backend.ai-common/issues/38))


20.03.0b5 (2020-06-07)
----------------------

### Fixes
* Fix mount expression type using an explicitly typed tuples in `types.KernelCreationConfig` ([#35](https://github.com/lablup/backend.ai-common/issues/35))


20.03.0b4 (2020-06-07)
----------------------

### Fixes
* Fix pickling exceptions imported later than the `ai.backend.common.logging` package ([#34](https://github.com/lablup/backend.ai-common/issues/34))


20.03.0b3 (2020-05-20)
----------------------

### Fixes
* `types`: Fix humanization of infinity values ([#32](https://github.com/lablup/backend.ai-common/issues/32))


20.03.0b2 (2020-05-12)
----------------------

### Miscellaneous
* Adopt [towncrier](https://github.com/twisted/towncrier) for changelog management ([#30](https://github.com/lablup/backend.ai-common/issues/30))
* Update flake8 to a prerelease supporting Python 3.8 syntaxes ([#31](https://github.com/lablup/backend.ai-common/issues/31))

20.03.0b1 (2020-03-19)
----------------------

### Breaking Changes
* `etcd.get_prefix()` now automatically unquote sub-keys returned as dictionaries and add `put_prefix()`
  which does the reverse while keeping `put_dict()` for backward compatibility ([#18](https://github.com/lablup/backend.ai-common/issues/18))

### Features
* Update `KernelCreationConfig` and `ServicePortProtocols` to support pre-open service ports ([#17](https://github.com/lablup/backend.ai-common/issues/17))
* Improve pickling of exceptions for new logging archiecture ([#22](https://github.com/lablup/backend.ai-common/issues/22))
* Add new type validators: JSONString and humanized TimeDuration ([#24](https://github.com/lablup/backend.ai-common/issues/24))
* Add a new utility function: get_random_seq() ([#25](https://github.com/lablup/backend.ai-common/issues/25))
* Add an asynchronous file writer wrapped as context manager via janus queues ([#26](https://github.com/lablup/backend.ai-common/issues/26))
* Add a monkey-patcher to enable pickling of trafaret.DataError objects ([#27](https://github.com/lablup/backend.ai-common/issues/27))

### Fixes
* Fix registry parsing: it had generated a bogus empty-key field when there are sub-kvpairs

### Miscellaneous
* Revamp CI: separate linting and type-checks using GitHub Actions ([#19](https://github.com/lablup/backend.ai-common/issues/19))
* Refactor out commonly used Redis/vfolder configurations ([#29](https://github.com/lablup/backend.ai-common/issues/29))

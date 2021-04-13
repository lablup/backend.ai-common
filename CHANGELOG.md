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

21.03.1 (2021-04-13)
--------------------

### Features
* Update for the manager scheduler v3 (lablup/backend.ai-manager#415) ([#76](https://github.com/lablup/backend.ai-common/issues/76))
  - Fix `LogSeverity` serialization error for `AgentErrorEvent`
  - Add new events for the updated manager scheduler (`SessionPreparingEvent`, `DoPrepareEvent`)


21.03.0 (2021-03-29)
--------------------

### Features
* Add msec to the console logs for debugging delay issues ([#74](https://github.com/lablup/backend.ai-common/issues/74))

### Miscellaneous
* Add an internal script (`scripts/diff-release.py`) to diff the release branches to check which PRs are backported or not ([#75](https://github.com/lablup/backend.ai-common/issues/75))


21.03.0.rc1 (2021-03-20)
------------------------

### Features
* Now it runs on Python 3.9 ([#67](https://github.com/lablup/backend.ai-common/issues/67))
* Migrate all event bus related codes into the new `common.events` module with statically typed event objects and explicit serialization and deserialization implementations ([#70](https://github.com/lablup/backend.ai-common/issues/70))

### Fixes
* Update package dependencies including coloredlogs, pyzmq, pytest and mypy ([#69](https://github.com/lablup/backend.ai-common/issues/69))
* Fix a typo in `validators.JsonWebToken`, the `algorithms` argument when decoding the token which became mandatory in PyJWT 2.0+ ([#71](https://github.com/lablup/backend.ai-common/issues/71))
* Update pyzmq to v22 series to reduce its wheel distribution size and fix a fork-safety bug introduced in v20. ([#72](https://github.com/lablup/backend.ai-common/issues/72))
* Remove no longer used `AgentStatsEvent` and make `HookResult` mutable ([#73](https://github.com/lablup/backend.ai-common/issues/73))


20.09.1 (2020-12-30)
--------------------

### Fixes
* Fix an edge case of HA setup with etcd, where a newly joined / failed-over etcd node does not recognize existing still-valid auth tokens ([#66](https://github.com/lablup/backend.ai-common/issues/66))


20.09.0 (2020-12-27)
--------------------

No significant changes.


20.09.0rc1 (2020-12-23)
-----------------------

No significant changes.


20.09.0b4 (2020-12-21)
----------------------

### Features
* Add `types.check_typed_dict()`  for generic TypedDict runtime validation and `types.HardwareMetadata` to support hardware metadata queries. ([#65](https://github.com/lablup/backend.ai-common/issues/65))


20.09.0b3 (2020-12-18)
----------------------

### Features
* Add `SlotTypes.UNIQUE` declaration ([#63](https://github.com/lablup/backend.ai-common/issues/63))

### Miscellaneous
* Unify CI workflows into GitHub Actions ([#64](https://github.com/lablup/backend.ai-common/issues/64))


20.09.0b2 (2020-12-09)
----------------------

### Features
* Add new trafaret-based validators `StringList` and `URL` ([#61](https://github.com/lablup/backend.ai-common/issues/61))

### Miscellaneous
* Update dependencies and further relax the dependency versions to adapt with the new pip resolver ([#62](https://github.com/lablup/backend.ai-common/issues/62))


20.09.0a2 (2020-10-30)
----------------------

### Features
* Add a finite-only constructor to `types.BinarySize`, which simplifies type handling when there are no infinite values expected ([#55](https://github.com/lablup/backend.ai-common/issues/55))

### Fixes
* Make `validators.BinarySize` commutative with other trafarets and ensure that the converted values are either `Decimal` (for infinity) and `BinarySize` ([#54](https://github.com/lablup/backend.ai-common/issues/54))
* Silence a bogus `PoolClosedError` from aioredis during shutting down servers ([#56](https://github.com/lablup/backend.ai-common/issues/56))

### Miscellaneous
* Update dependencies, including upgrades to aiohttp 3.7 and aiotools 1.0 ([#57](https://github.com/lablup/backend.ai-common/issues/57))
* Use `towncrier.check` in replacement of psf-chronographer ([#58](https://github.com/lablup/backend.ai-common/issues/58))


20.09.0a1 (2020-10-06)
----------------------

### Features
* Add `validators.PurePath`, a simpelified and non-syscall-involving version of `validators.Path` ([#44](https://github.com/lablup/backend.ai-common/issues/44))
* Implement an ultimate exception pickler to log arbitrary errors, `PickledException`, which carries the `repr()`-ed original exception ([#45](https://github.com/lablup/backend.ai-common/issues/45))
* Add JWT (JSON Web Token) validator (`tx.JsonWebToken`) with an inner token-data validator option ([#46](https://github.com/lablup/backend.ai-common/issues/46))
* Add support for `context` as optional argument, e.g. used to pass on instances at initialization, for the plugin subsystem. ([#47](https://github.com/lablup/backend.ai-common/issues/47))
* Add types for kernel clsutering support ([#48](https://github.com/lablup/backend.ai-common/issues/48))
* Add `ai.backend.json.ExtendedJSONEncoder` to handle UUID and datetime objects transparently when encoding objetcs into JSON ([#50](https://github.com/lablup/backend.ai-common/issues/50))
* Add `service_ports` module to place the common routines for parsing service ports declarations ([#52](https://github.com/lablup/backend.ai-common/issues/52))

### Fixes
* Fix error monitor plugin and plugin context to match the updated API ([#47](https://github.com/lablup/backend.ai-common/issues/47))
* Make the mount path in the vfolder etcd config validator nullable to allow detection of storage-proxy configs in agents ([#51](https://github.com/lablup/backend.ai-common/issues/51))


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

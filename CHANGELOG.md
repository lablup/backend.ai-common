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

## 22.03.1 (2022-05-24)

### Fixes
* Add explicit error logs to inform occurrence of `aioredis.exceptions.ConnectionError` because incorrect server URLs and temporary network failures are indistinguishable but should be diagnosable ([#139](https://github.com/lablup/backend.ai-common/issues/139))
* Fix mypy failing on `EtcdLock` and `AsyncEtcd`. ([#141](https://github.com/lablup/backend.ai-common/issues/141))

### Miscellaneous
* Bump etcetra version to 0.1.6. ([#142](https://github.com/lablup/backend.ai-common/issues/142))


## 22.03.0 (2022-04-25)

### Miscellaneous
* Update aiotools to 1.5.8 fixing subprocess spawn errors with pidfd-enabled setups and the asyncio event-loop option.


## 22.03.0b5 (2022-04-18)

### Miscellaneous
* Explicitly show an warning log upon timeout of `GlobalTimer` acquiring a distributed lock and increase the default lock acquire timeout to 9600 seconds ([#138](https://github.com/lablup/backend.ai-common/issues/138))


## 22.03.0b4 (2022-04-12)

### Features
* Let `validators.TimeDuration` to accept numbers (int and float) as well ([#128](https://github.com/lablup/backend.ai-common/issues/128))
* Migrate `manager.distribute` and make it to use pluggable distributed lock implementations ([#129](https://github.com/lablup/backend.ai-common/issues/129))
* Allow `EventDispatcher` and `EventProducer` to have a custom stream key other than "events" ([#131](https://github.com/lablup/backend.ai-common/issues/131))
* Add `EtcdLock`, a locking mechanism based on Etcd's lock feature. ([#133](https://github.com/lablup/backend.ai-common/issues/133))
* Add lifetime to the distributed locks to ensure the liveness property (i.e., guaranteed automatic release of locks in case of hangs or network partitions) ([#136](https://github.com/lablup/backend.ai-common/issues/136))

### Fixes
* Prevent a potential deadlock when all executor threads is occupied by the lock acquiring methods and unlocking via the executor becomes impossible ([#132](https://github.com/lablup/backend.ai-common/issues/132))
* Migrate the bgtask framework from the manager and make it reusable in other projects such as storage-proxy ([#135](https://github.com/lablup/backend.ai-common/issues/135))

### Miscellaneous
* Remove old codes in ImageRef class, which was used to perform Etcd operations. ([#127](https://github.com/lablup/backend.ai-common/issues/127))
* Run the redis test suite only when modules under common.redis or their test codes are changed ([#130](https://github.com/lablup/backend.ai-common/issues/130))


## 22.03.0b3 (2022-03-29)

### Features
* Replace `aetcd3` library with `etcetra`. ([#124](https://github.com/lablup/backend.ai-common/issues/124))
* Accept async pipeline builders in `redis.execute()` instead of silently failing by returning pipeline objects ([#125](https://github.com/lablup/backend.ai-common/issues/125))

### Fixes
* Fix argument typing of `redis.execute_script()` ([#126](https://github.com/lablup/backend.ai-common/issues/126))


## 22.03.0b2 (2022-03-14)

### Breaking Changes
* Retire pre-storage-proxy-era mount tuple data types ([#121](https://github.com/lablup/backend.ai-common/issues/121))
* Now the minimum required Python version is 3.10.2. ([#123](https://github.com/lablup/backend.ai-common/issues/123))

### Features
* Add architecture name alias mapping to convert `manager.machine()` output as docker API's norm. ([#119](https://github.com/lablup/backend.ai-common/issues/119))
* Add `types.JSONSerializableMixin` for defining attr-based dataclasses with trafaret-based deserialization ([#121](https://github.com/lablup/backend.ai-common/issues/121))
* Add more fields to `types.VFolderMount` as a follow-up of #121 ([#122](https://github.com/lablup/backend.ai-common/issues/122))

### Miscellaneous
* Upgrade aiotools to 1.5 series for improvements of `TaskGroup` and `PersistentTaskGroup` ([#120](https://github.com/lablup/backend.ai-common/issues/120))


## 22.03.0b1 (2022-02-28)

### Features
* Migrate file-based lock code from storage-proxy package ([#98](https://github.com/lablup/backend.ai-common/issues/98))
* Update `validators.TimeDuration` class to support years and months ([#99](https://github.com/lablup/backend.ai-common/issues/99))
* Add `architecture` variable on `ImageRef` class to support multi-architecture image. ([#118](https://github.com/lablup/backend.ai-common/issues/118))

### Fixes
* Remove an unreachable statement in `BinarySize.__format__()` ([#100](https://github.com/lablup/backend.ai-common/issues/100))
* Fix minor typing errors discovered by mypy 0.920 update ([#104](https://github.com/lablup/backend.ai-common/issues/104))
* Update mypy to 0.930 and fix newly discovered type errors ([#105](https://github.com/lablup/backend.ai-common/issues/105))
* Fix potential deadlock upon shutdown of service daemons ([#106](https://github.com/lablup/backend.ai-common/issues/106))
* Fix typing issues of `StringSetFlag` by refactoring it using a separate interface definition file ([#107](https://github.com/lablup/backend.ai-common/issues/107))
* fix file logging crashing when backup-count option not set ([#108](https://github.com/lablup/backend.ai-common/issues/108))
* Use a fixed value for the Redis stream consumer ID in `EventDispatcher` to avoid accumulation of unused consumers upon service restarts, which causes database transaction flooding and slow-down of event processing ([#109](https://github.com/lablup/backend.ai-common/issues/109))
* Improve `EventDispatcher` stability by earlier `XACK` and explicit `XDEL` Redis stream commands ([#110](https://github.com/lablup/backend.ai-common/issues/110))
* Adopt `aiotools.PersistentTaskGroup` to manager event handler tasks in `EventDispatcher` to capture unhandled exceptions explicitly and reduce boilerplate codes ([#111](https://github.com/lablup/backend.ai-common/issues/111))
* Import and update `EventDispatcher` test cases from the manager sources with a minor refactoring to add new optional constructor arguments for custom exception handlers ([#112](https://github.com/lablup/backend.ai-common/issues/112))
* Improve stability of `EventDispatcher`'s pub/sub messaging pattern using proper `last_id` and `XTRIM` command as a stream garbage collector ([#114](https://github.com/lablup/backend.ai-common/issues/114))
* Improve the retry condition check for Redis command execution failures when used with pipelines ([#115](https://github.com/lablup/backend.ai-common/issues/115))
* Silence a typing error in the `config` module due to `toml.decoder`'s way of tagging dict objects at runtime to distinguish inline and non-inline tables ([#117](https://github.com/lablup/backend.ai-common/issues/117))

### Miscellaneous
* Add a test case about using Redis pipelines with `types.RedisConnectionInfo` and `redis.execute()` as an example ([#113](https://github.com/lablup/backend.ai-common/issues/113))
* Update pip caching in GitHub Actions to use the scheme managed by actions/setup-python ([#116](https://github.com/lablup/backend.ai-common/issues/116))


## Older changelogs

* [21.09](https://github.com/lablup/backend.ai-common/blob/21.09/CHANGELOG.md)
* [21.03](https://github.com/lablup/backend.ai-common/blob/21.03/CHANGELOG.md)
* [20.09](https://github.com/lablup/backend.ai-common/blob/20.09/CHANGELOG.md)
* [20.03](https://github.com/lablup/backend.ai-common/blob/20.03/CHANGELOG.md)


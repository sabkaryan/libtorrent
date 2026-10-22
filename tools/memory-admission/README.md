# Native admission checks

Initialize `deps/try_signal` at its tracked commit. Configure Boost/OpenSSL paths
for the machine. `CMAKE` and `PYTHON` may name absolute executables.

```sh
tools/memory-admission/run.sh /tmp/admission-build \
  -DBoost_INCLUDE_DIR=/path/to/boost -DBoost_NO_BOOST_CMAKE=ON \
  -DOPENSSL_ROOT_DIR=/path/to/openssl
tools/memory-admission/verify.sh /tmp/admission-build /tmp/admission-evidence
```

`run.sh` configures Debug, assertions and full invariant checks, builds the four
memory suites and runs them. `verify.sh` applies eleven distinct production
operators with thirteen named target checks (M1b repeats M1 for the
foreign-torrent test, M11b repeats M11 for the failed partial transfer),
restores each source in `finally`, rebuilds the restored memory suites,
then builds and runs the entire CMake test project. `FULL_GATE=0` skips only that
last, broad step when repeating the mutations after formatting. A failed mutation
or failed restored memory suite stops the broad step. The broad timeout is 600 s
per test executable.

A mutant counts as killed only when the replacement matched exactly once, the
suite compiled, the exact named test ran, exited with the native framework's
failure code 77 and reported an assertion failure. A compile error, timeout,
signal or missing test is not TARGET RED. Every source is restored
unconditionally.

Outputs include per-mutation build/test logs, `summary.log`,
`restored-suite.log`, `full-build.log` and `full-suite.log`. Start a new output
directory for an independent series. The scripts do not tag a release or merge
a branch.

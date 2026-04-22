# Testing

Tests are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html) in `test/sql/`.

```sh
just test         # build + run all tests
```

Or via make directly:
```sh
make test_debug
make test_release
```

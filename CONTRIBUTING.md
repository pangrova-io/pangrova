# Contributing to Pangrova

Thanks for your interest in contributing to Pangrova. This guide covers everything you need to get started.

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## Reporting Issues

1. Search [existing issues](https://github.com/pangrova-project/pangrova/issues) first
2. Create a new issue with: reproduction steps, environment info (OS, JDK version), and error logs
3. Use the appropriate issue template (bug report / feature request)

## Development Setup

### Required Tools

| Tool | Version | Purpose |
|------|---------|---------|
| JDK | 17+ | Java compilation and runtime |
| Maven | 3.8+ | Build management |
| Docker | 20+ | Integration tests (TestContainers) |
| Git | 2.30+ | Version control |
| CMake + Ninja | 3.20+ | Native C++ build (optional) |

### Build

```bash
git clone https://github.com/pangrova-project/pangrova.git
cd pangrova
mvn clean install -DskipTests
```

### Run Tests

```bash
# All tests (requires Docker for TestContainers)
mvn test

# Single module
mvn -pl pangrova-coordinator test

# Single test class
mvn -pl pangrova-coordinator -Dtest=QueryDispatcherTest test
```

### IDE

IntelliJ IDEA recommended. After importing the root `pom.xml`:
- Set Project SDK to JDK 17+
- Disable wildcard imports: Settings > Code Style > Java > Imports > set class count to 999
- Install Protobuf plugin for `.proto` file support

## Submitting Changes

### Branch Naming

```
feature/{short-description}
fix/{short-description}
docs/{short-description}
refactor/{short-description}
```

### Workflow

1. Fork the repository
2. Create a branch from `main`
3. Make your changes
4. Run `mvn clean install` (all tests must pass)
5. Push and create a Pull Request

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

<body>

<footer>
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `perf`

## Code Standards

### Java

- Constructor injection only — no `@Autowired`, no Spring DI
- YAML configuration only — no `.properties` files
- Immutable DTOs — use `record` or `final` fields with builder
- Method ≤ 50 lines, class ≤ 500 lines, ≤ 3 indent levels
- Constructor deps ≤ 4, method params ≤ 5
- No `*Impl` suffix — use descriptive names
- Structured logging with `key=value` format
- Public API entry points must validate null/range

### Testing

- No mock frameworks (Mockito, PowerMock, EasyMock are banned)
- No `@Disabled` tests — fix or delete
- No `java.lang.reflect` in tests
- Use TestContainers for external dependencies
- Use jqwik for property-based testing of core logic
- Naming: `*Test.java`, `*IntegrationTest.java`, `*PropertyTest.java`
- Property tests go in `property` sub-package

### C++ / Protobuf

- C++ files: `PascalCase.h/.cpp`, namespace `pangrova::*`, smart pointers
- Proto files: `snake_case.proto`, messages `PascalCase`, fields `snake_case`

### Module Dependencies

Strictly enforced. See `pangrova-rules.md` for the full matrix. Key rules:
- `spi`/`protocol` must not depend on implementation modules
- `connector-*` may only depend on `spi`, `common`, `protocol`
- Cross-module communication goes through SPI interfaces

## PR Checklist

- [ ] Code compiles: `mvn clean install -DskipTests`
- [ ] All tests pass: `mvn test`
- [ ] New features have tests
- [ ] No new compiler warnings
- [ ] No new dependencies without justification
- [ ] Documentation updated if behavior changed
- [ ] Commit messages follow conventional format
- [ ] No `@Disabled` tests added
- [ ] No mock frameworks used

## License

Contributions are licensed under [Apache License 2.0](LICENSE).

"""Exercise native sanitizer dependency qualification and probe execution."""

from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
MODULE = ROOT / "pangrova-worker-native/cmake/PangrovaRe2.cmake"


class NativeSanitizerProbeTest(unittest.TestCase):
    def run_probe(self, apple, exit_code=0):
        with tempfile.TemporaryDirectory(prefix="native sanitizer probe ") as directory:
            root = Path(directory)
            package = root / "deps/lib/cmake/re2"
            package.mkdir(parents=True)
            (package / "re2Config.cmake").write_text("# Test package marker\n")
            sdk = root / "sdk"
            sdk.mkdir()
            (sdk / "CMakeCache.txt").write_text(f"re2_DIR:PATH={package}\n")
            output = root / "child-environment"
            probe = root / "probe"
            probe.write_text("#!/bin/sh\n"
                             f"printf '%s' \"$ASAN_OPTIONS\" > {shlex.quote(str(output))}\n"
                             f"exit {exit_code}\n")
            probe.chmod(0o755)
            # Substitute only dependency discovery/compilation. The production
            # module still launches the child through its real cmake -E env and
            # must propagate its actual process result. This is not an ABI test.
            (root / "CMakeLists.txt").write_text(f"""
cmake_minimum_required(VERSION 3.20)
project(ProbeEnvironment NONE)
set(APPLE {"TRUE" if apple else "FALSE"})
set(PANGROVA_DEPS_PREFIX "{root / 'deps'}")
set(VELOX_BUILD_DIR "{sdk}")
function(find_package package)
  if(package STREQUAL "re2")
    add_library(re2::re2 INTERFACE IMPORTED)
    set_property(TARGET re2::re2 PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
    set_property(TARGET re2::re2 PROPERTY IMPORTED_LOCATION_RELEASE unused.fixture)
  endif()
endfunction()
function(try_compile result directory)
  file(MAKE_DIRECTORY "${{directory}}")
  file(COPY_FILE "{probe}" "${{directory}}/pangrova_re2_contract")
  set(${{result}} TRUE PARENT_SCOPE)
endfunction()
include("{MODULE}")
""")
            result = subprocess.run(["cmake", "-S", str(root), "-B", str(root / "build")],
                                    text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    check=False)
            return result, output.read_text() if output.exists() else None

    def test_darwin_child_runs_address_checks(self):
        result, environment = self.run_probe(apple=True)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(environment, "detect_leaks=0:halt_on_error=1")

    def test_linux_child_retains_leak_checks(self):
        result, environment = self.run_probe(apple=False)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(environment, "detect_leaks=1:halt_on_error=1")

    def test_failed_child_still_rejects_the_configuration(self):
        result, environment = self.run_probe(apple=True, exit_code=23)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Canonical RE2 runtime ABI mismatch (23)", result.stdout)
        self.assertEqual(environment, "detect_leaks=0:halt_on_error=1")


class AsanFollyContractTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="asan folly contract ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        source = (ROOT / "pangrova-worker-native/cmake/FindVelox.cmake").read_text()
        start = source.index('set(PANGROVA_ASAN_FOLLY_PREFIX "" CACHE PATH')
        end = source.index("# 3a) deps-install")
        self.assertLess(start, end, "Folly qualification must not depend on deps-install")
        self.contract = source[start:end]

    def package(self, instrumented=True, headers=True):
        prefix = self.root / "folly install"
        library = prefix / "lib/libfolly.a"
        library.parent.mkdir(parents=True)
        source = self.root / "folly.cpp"
        source.write_text('extern "C" int read_value(int* value) { return *value; }\n')
        obj = self.root / "folly.o"
        flags = ["-fsanitize=address,undefined"] if instrumented else []
        subprocess.run(["c++", *flags, "-c", str(source), "-o", str(obj)], check=True,
                       capture_output=True, text=True)
        subprocess.run(["ar", "rcs", str(library), str(obj)], check=True,
                       capture_output=True, text=True)
        if headers:
            include = prefix / "include/folly"
            include.mkdir(parents=True)
            (include / "Executor.h").touch()
        gflags = prefix / "lib/cmake/gflags"
        gflags.mkdir(parents=True)
        (gflags / "gflags-config.cmake").write_text(
            'add_library(gflags_static INTERFACE IMPORTED)\n')
        config = prefix / "lib/cmake/folly"
        config.mkdir(parents=True)
        flags = "-fsanitize=address,undefined;" if instrumented else ""
        (config / "folly-config.cmake").write_text(f'''
add_library(Folly::folly STATIC IMPORTED)
set_target_properties(Folly::folly PROPERTIES
  IMPORTED_LOCATION "{library}"
  INTERFACE_LINK_LIBRARIES "{flags}gflags_static")
''')
        return prefix

    def configure(self, prefix=None, asan=True, apple=None):
        settings = f'set(PANGROVA_ASAN_FOLLY_PREFIX "{prefix}" CACHE PATH "")\n' if prefix else ""
        if apple is not None:
            settings += f'set(APPLE {"TRUE" if apple else "FALSE"})\n'
        if asan:
            settings += 'set(CMAKE_CXX_FLAGS "-fsanitize=address")\n'
        settings += f'set(CMAKE_PREFIX_PATH "{self.root / "folly install"}")\n'
        (self.root / "main.cpp").write_text(
            'extern "C" int read_value(int*);\n'
            'int main() { int value = 7; return read_value(&value) == 7 ? 0 : 1; }\n')
        (self.root / "CMakeLists.txt").write_text(
            'cmake_minimum_required(VERSION 3.20)\nproject(FollyContract CXX)\n'
            'add_library(velox_imported INTERFACE)\n' + settings + self.contract +
            '\nadd_executable(folly_consumer main.cpp)\n'
            'target_link_libraries(folly_consumer PRIVATE velox_imported)\n')
        return subprocess.run(["cmake", "-S", str(self.root), "-B", str(self.root / "build")],
                              capture_output=True, text=True)

    def test_missing_prefix_is_rejected_without_a_deps_install_directory(self):
        result = self.configure()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ASan native builds require PANGROVA_ASAN_FOLLY_PREFIX", result.stderr)

    def test_release_folly_archive_is_rejected(self):
        result = self.configure(self.package(instrumented=False))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not contain an ASan-instrumented Folly archive",
                      " ".join(result.stderr.split()))

    def test_missing_matching_headers_are_rejected(self):
        result = self.configure(self.package(headers=False))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ASan Folly install is incomplete", result.stderr)

    def test_release_consumer_cannot_select_asan_folly(self):
        result = self.configure(self.package(), asan=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ASan Folly override requires an ASan native build", result.stderr)

    def test_instrumented_target_propagates_both_runtimes_to_a_real_consumer(self):
        result = self.configure(self.package())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        result = subprocess.run(["cmake", "--build", str(self.root / "build")],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        result = subprocess.run([str(self.root / "build/folly_consumer")],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_linux_release_consumer_imports_the_installed_folly_package(self):
        self.package(instrumented=False)
        result = self.configure(asan=False, apple=False)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        result = subprocess.run(["cmake", "--build", str(self.root / "build")],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        result = subprocess.run([str(self.root / "build/folly_consumer")],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_linux_release_consumer_uses_the_shared_gflags_import(self):
        shared = self.root / "libgflags.so"
        (self.root / "CMakeLists.txt").write_text(f"""
cmake_minimum_required(VERSION 3.20)
project(GflagsContract NONE)
set(APPLE FALSE)
set(PANGROVA_PRODUCTION_BUILD ON)
add_library(gflags::gflags STATIC IMPORTED)
set_target_properties(gflags::gflags PROPERTIES
  IMPORTED_CONFIGURATIONS RELEASE
  IMPORTED_LOCATION_RELEASE "{self.root / 'libgflags.a'}")
function(find_library variable)
  set(${{variable}} "{shared}" PARENT_SCOPE)
endfunction()
{self.contract}
_pangrova_prefer_shared_gflags()
file(GENERATE OUTPUT "${{CMAKE_CURRENT_BINARY_DIR}}/gflags-path.txt"
  CONTENT "$<TARGET_FILE:gflags::gflags>")
""")
        build = self.root / "build"
        result = subprocess.run(["cmake", "-S", str(self.root), "-B", str(build),
                                 "-DCMAKE_BUILD_TYPE=Release"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((build / "gflags-path.txt").read_text(), str(shared))

    def test_linux_production_links_only_the_canonical_gflags_archive(self):
        source = (ROOT / "pangrova-worker-native/cmake/FindVelox.cmake").read_text()
        start = source.index("# 6) glog + gflags")
        end = source.index("# 7) AWS SDK", start)
        prefix = self.root / "production dependencies"
        archive = prefix / "lib/libgflags.a"
        (self.root / "CMakeLists.txt").write_text(f"""
cmake_minimum_required(VERSION 3.20)
project(ProductionGflags NONE)
set(APPLE FALSE)
set(PANGROVA_PRODUCTION_BUILD ON)
set(PANGROVA_DEPS_PREFIX "{prefix}")
add_library(velox_imported INTERFACE)
function(find_library variable)
  if(variable STREQUAL "_SYS_GFLAGS" OR variable STREQUAL "_pangrova_gflags_shared")
    message(FATAL_ERROR "Production must not search for system gflags")
  endif()
endfunction()
{source[start:end]}
get_property(libraries TARGET velox_imported PROPERTY INTERFACE_LINK_LIBRARIES)
file(WRITE "${{CMAKE_CURRENT_BINARY_DIR}}/gflags-path.txt" "${{libraries}}")
""")
        build = self.root / "build"
        command = ["cmake", "-S", str(self.root), "-B", str(build)]
        missing = subprocess.run(command, capture_output=True, text=True)
        self.assertNotEqual(missing.returncode, 0)
        self.assertIn("Linux production requires the PIC gflags archive", missing.stderr)
        archive.parent.mkdir(parents=True)
        archive.touch()
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((build / "gflags-path.txt").read_text(), str(archive))

    def test_dependency_globs_cannot_reintroduce_release_folly(self):
        source = (ROOT / "pangrova-worker-native/cmake/FindVelox.cmake").read_text()
        for variable in ("_DEPS_INSTALL_LIBS", "_VENDORED_LIBS", "_INSTALL_PREFIX_LIBS"):
            selection = re.search(r'if\(PANGROVA_ASAN_FOLLY_PREFIX\)\s+'
                                  r'list\(FILTER ' + variable + r' EXCLUDE REGEX [^\n]+\)\s+'
                                  r'endif\(\)', source)
            self.assertIsNotNone(selection, variable)
            for enabled in (True, False):
                with self.subTest(variable=variable, asan=enabled):
                    archives = "/release/libfolly.a;/release/libfollybase.a;/keep/libother.a"
                    expected = "/keep/libother.a" if enabled else archives
                    script = self.root / "selection.cmake"
                    script.write_text(
                        'cmake_minimum_required(VERSION 3.20)\n'
                        f'set(PANGROVA_ASAN_FOLLY_PREFIX {"ON" if enabled else "OFF"})\n'
                        f'set({variable} "{archives}")\n' + selection[0] +
                        f'\nif(NOT "${{{variable}}}" STREQUAL "{expected}")\n'
                        '  message(FATAL_ERROR "Unexpected Folly archive selection")\nendif()\n')
                    result = subprocess.run(["cmake", "-P", str(script)],
                                            capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_bootstrap_and_ci_forward_the_same_folly_prefix(self):
        setup = (ROOT / "pangrova-worker-native/bin/setup-native-asan-env.sh").read_text()
        self.assertLess(setup.index("\nbuild_asan_folly\n"),
                        setup.index("\nconfigure_asan_velox\n"))
        self.assertIn("-DFOLLY_LIBRARY_SANITIZE_ADDRESS=ON", setup)
        self.assertLess(setup.index("--target arrow_ep-patch"),
                        setup.index("arrow_source="))
        self.assertIn("-Dfolly_SOURCE=SYSTEM", setup)
        self.assertIn('export PANGROVA_ASAN_FOLLY_PREFIX="$PANGROVA_ASAN_FOLLY_PREFIX"', setup)
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        self.assertIn('-DPANGROVA_ASAN_FOLLY_PREFIX="$PANGROVA_ASAN_FOLLY_PREFIX"', workflow)


class AsanArrowSourceTest(unittest.TestCase):
    def test_external_project_inherits_compilers_and_sanitizer_flags(self):
        with tempfile.TemporaryDirectory(prefix="asan-arrow-flags-") as tmp:
            root = Path(tmp)
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            resolver = root / 'CMake/resolve_dependency_modules/arrow'
            resolver.mkdir(parents=True)
            child = root / 'arrow-source'
            child.mkdir()
            (root / 'CMakeLists.txt').write_text('''cmake_minimum_required(VERSION 3.28)
project(ArrowBuildFlags C CXX)
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_C_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_C_FLAGS_RELEASE "-O1 -g1 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O1 -g1 -DNDEBUG")
set(CMAKE_EXE_LINKER_FLAGS "-fsanitize=address")
set(CMAKE_SHARED_LINKER_FLAGS "-fsanitize=address")
add_subdirectory(CMake/resolve_dependency_modules/arrow)
''')
            (resolver / 'CMakeLists.txt').write_text('''include(ExternalProject)
set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep")
set(ARROW_CMAKE_ARGS
    -DCMAKE_INSTALL_LIBDIR=lib
    -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}/install
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DARROW_BUILD_STATIC=ON
    -DThrift_SOURCE=${THRIFT_SOURCE}
    -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}
)
ExternalProject_Add(arrow_ep
  SOURCE_DIR "${CMAKE_SOURCE_DIR}/arrow-source"
  BINARY_DIR "${CMAKE_BINARY_DIR}/arrow-child"
  CMAKE_ARGS ${ARROW_CMAKE_ARGS}
  INSTALL_COMMAND "")
''')
            (child / 'CMakeLists.txt').write_text('''cmake_minimum_required(VERSION 3.28)
project(ArrowChild C CXX)
foreach(language C CXX)
  if(NOT CMAKE_${language}_FLAGS MATCHES "-fsanitize=address")
    message(FATAL_ERROR "Arrow child lost ASan flags")
  endif()
  if(NOT CMAKE_${language}_FLAGS_RELEASE STREQUAL "-O1 -g1 -DNDEBUG")
    message(FATAL_ERROR "Arrow child lost sanitizer optimization flags")
  endif()
endforeach()
foreach(kind EXE SHARED)
  if(NOT CMAKE_${kind}_LINKER_FLAGS STREQUAL "-fsanitize=address")
    message(FATAL_ERROR "Arrow child lost sanitizer linker flags")
  endif()
endforeach()
add_library(arrow_probe STATIC probe.c probe.cpp)
add_executable(arrow_consumer main.cpp)
target_link_libraries(arrow_consumer PRIVATE arrow_probe)
''')
            (child / 'probe.c').write_text('int read_c(int* value) { return *value; }\n')
            (child / 'probe.cpp').write_text('int read_cpp(int* value) { return *value; }\n')
            (child / 'main.cpp').write_text(
                'extern "C" int read_c(int*);\nint read_cpp(int*);\n'
                'int main() { int value = 7; return read_c(&value) == read_cpp(&value) ? 0 : 1; }\n')
            build = root / 'build'
            configure = ['cmake', '-S', str(root), '-B', str(build)]
            compile_command = ['cmake', '--build', str(build), '--target', 'arrow_ep', '--parallel', '1']
            subprocess.run(configure, check=True, capture_output=True, text=True)
            failed = subprocess.run(compile_command, capture_output=True, text=True)
            self.assertNotEqual(failed.returncode, 0)
            self.assertIn('Arrow child lost ASan flags', failed.stdout + failed.stderr)
            setup = (ROOT / 'pangrova-worker-native/bin/setup-native-asan-env.sh').read_text()
            start = setup.index('arrow_build_patch=')
            end = setup.index('\nmkdir -p "$ASAN_PREFIX"', start)
            shell = ('set -euo pipefail\n'
                     f'VELOX_HOME={shlex.quote(str(root))}\n'
                     f'ASAN_SCRIPT_DIR={shlex.quote(str(ROOT / "pangrova-worker-native/bin"))}\n' +
                     setup[start:end])
            for _ in range(2):
                subprocess.run(['/bin/bash', '-c', shell], check=True, capture_output=True, text=True)
            subprocess.run(configure, check=True, capture_output=True, text=True)
            result = subprocess.run(compile_command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            parent_cache = (build / 'CMakeCache.txt').read_text()
            child_cache = (build / 'arrow-child/CMakeCache.txt').read_text()
            for language in ('C', 'CXX'):
                pattern = rf'^CMAKE_{language}_COMPILER:[^=]+=(.+)$'
                self.assertEqual(re.search(pattern, parent_cache, re.M)[1],
                                 re.search(pattern, child_cache, re.M)[1])
            symbols = subprocess.check_output(['nm', '-u', str(build / 'arrow-child/libarrow_probe.a')],
                                              text=True)
            self.assertRegex(symbols, r'_+asan_init')
            subprocess.run([str(build / 'arrow-child/arrow_consumer')], check=True,
                           capture_output=True, text=True)

    def test_bootstrap_generates_patch_target_despite_ambient_system_arrow(self):
        setup = (ROOT / "pangrova-worker-native/bin/setup-native-asan-env.sh").read_text()
        option = re.search(r'-DCMAKE_PROJECT_Arrow_INCLUDE:FILEPATH=\$ASAN_SCRIPT_DIR/\.\./(\S+?)"',
                           setup)
        self.assertIsNotNone(option, "ASan bootstrap must wire the Velox Arrow project include")
        hook = ROOT / "pangrova-worker-native" / option[1]
        configure = re.search(r'^  ((?:Arrow_SOURCE=[A-Z]+ )?cmake) -S "\$VELOX_HOME"', setup, re.M)
        self.assertIsNotNone(configure, "ASan bootstrap must configure Velox")
        self.assertLess(setup.index("--target arrow_ep-patch"), setup.index("--target velox"))
        with tempfile.TemporaryDirectory(prefix="asan-arrow-source-") as tmp:
            root = Path(tmp)
            (root / "arrow").mkdir()
            (root / "arrow-source").mkdir()
            (root / "arrow-source/source.txt").write_text("unpadded source")
            (root / "CMakeLists.txt").write_text("""cmake_minimum_required(VERSION 3.20)
project(Velox NONE)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}")
# Mirror the pinned Velox resolver's environment-over-cache precedence and AUTO discovery.
set(source "$ENV{Arrow_SOURCE}")
if(source STREQUAL "AUTO" OR source STREQUAL "SYSTEM")
  find_package(Arrow REQUIRED)
  return()
endif()
if(NOT source STREQUAL "BUNDLED")
  message(FATAL_ERROR "Unexpected Arrow_SOURCE: ${source}")
endif()
add_subdirectory(arrow)
""")
            (root / "FindArrow.cmake").write_text('set(Arrow_FOUND TRUE)\n')
            # Mirror the pinned nested project and ExternalProject name. No
            # STEP_TARGETS declaration, native builds, or network downloads.
            (root / "arrow/CMakeLists.txt").write_text("""project(Arrow NONE)
include(ExternalProject)
ExternalProject_Add(arrow_ep
  SOURCE_DIR "${CMAKE_SOURCE_DIR}/arrow-source"
  PATCH_COMMAND "${CMAKE_COMMAND}" -E touch "<SOURCE_DIR>/patch-ran"
  CONFIGURE_COMMAND "${CMAKE_COMMAND}" -E false
  BUILD_COMMAND "${CMAKE_COMMAND}" -E false
  INSTALL_COMMAND "${CMAKE_COMMAND}" -E false)
""")
            build = root / "build"
            for cmd in (["bash", "-c", 'export Arrow_SOURCE=SYSTEM; ' + configure[1] + ' "$@"',
                         "configure-asan-arrow", "-S", str(root), "-B", str(build),
                         f"-DCMAKE_PROJECT_Arrow_INCLUDE:FILEPATH={hook}"],
                        ["cmake", "--build", str(build), "--target", "arrow_ep-patch"]):
                result = subprocess.run(cmd, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertTrue((root / "arrow-source/patch-ran").is_file())


class FollySocketPatchTest(unittest.TestCase):
    def test_patch_preserves_function_availability_checks_with_sanitizers(self):
        patch = ROOT / "pangrova-worker-native/patches/folly-2026.01.05-socket-function-types.patch"
        with tempfile.TemporaryDirectory(prefix="folly socket patch ") as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            source = root / "deps-download/folly/folly/net/NetOps.cpp"
            source.parent.mkdir(parents=True)
            source.write_text('''#include <cstddef>
#include <stdexcept>

#include <folly/CPortability.h>
#include <folly/ScopeGuard.h>
#include <folly/Utility.h>

#ifdef _WIN32
#include <MSWSock.h> // @manual
#endif

#if (defined(__linux__) && !defined(__ANDROID__)) ||                       \\
    (defined(__ANDROID__) && __ANDROID_API__ >= 21 /* released 2014 */) || \\
    defined(__FreeBSD__) || defined(__SGX__) || defined(__EMSCRIPTEN__)
static_assert(folly::to_bool(::recvmmsg));
static_assert(folly::to_bool(::sendmmsg));
#else
static int (*recvmmsg)(...) = nullptr;
static int (*sendmmsg)(...) = nullptr;
#endif
''')
            command = ["patch", "--batch", "-d", str(root / "deps-download/folly"),
                       "-p1", "-i", str(patch)]
            subprocess.run([*command, "--dry-run", "--forward"], check=True, capture_output=True)
            setup = (ROOT / "pangrova-worker-native/bin/setup-native-asan-env.sh").read_text()
            start = setup.index("    if ! patch --dry-run",
                                setup.index('    local socket_patch="'))
            patch_step = setup[start:setup.index("    local architecture_flags", start)]
            shell = ('set -euo pipefail\n'
                     f'FOLLY_SOURCE={shlex.quote(str(root / "deps-download/folly"))}\n'
                     f'socket_patch={shlex.quote(str(patch))}\n' + patch_step)
            for _ in range(2):
                subprocess.run(["/bin/bash", "-c", shell], check=True, capture_output=True)
            subprocess.run([*command, "--dry-run", "--forward", "--reverse"],
                           check=True, capture_output=True)
            patched = source.read_text()
            self.assertIn("#include <type_traits>", patched)
            checks = "\n".join(line for line in patched.splitlines() if line.startswith("static_assert"))
            self.assertEqual(len(checks.splitlines()), 2)
            for declarations, valid in (
                    ("int recvmmsg(); int sendmmsg();", True),
                    ("int (*recvmmsg)(); int (*sendmmsg)();", False),
                    ("int sendmmsg();", False)):
                with self.subTest(declarations=declarations):
                    result = subprocess.run(
                        ["c++", "-std=c++20", "-fsanitize=address,undefined", "-fsyntax-only",
                         "-x", "c++", "-"], input="#include <type_traits>\n" + declarations + "\n" + checks,
                        text=True, capture_output=True)
                    self.assertEqual(result.returncode == 0, valid, result.stderr)


class FollyOpenSslPatchTest(unittest.TestCase):
    def test_patch_removes_direct_access_to_opaque_asn1_strings(self):
        patch = ROOT / "pangrova-worker-native/patches/folly-2026.01.05-openssl-asn1.patch"
        with tempfile.TemporaryDirectory(prefix="folly openssl patch ") as directory:
            root = Path(directory)
            source = root / "folly/io/async/ssl/OpenSSLUtils.cpp"
            source.parent.mkdir(parents=True)
            source.write_text("""    auto name = sk_GENERAL_NAME_value(altNames, i);
    if ((addr4 != nullptr || addr6 != nullptr) && name->type == GEN_IPADD) {
      // Extra const-ness for paranoia
      unsigned char const* const rawIpStr = name->d.iPAddress->data;
      auto const rawIpLen = size_t(name->d.iPAddress->length);

      if (rawIpLen == 4 && addr4 != nullptr) {
        if (::memcmp(rawIpStr, &addr4->sin_addr, rawIpLen) == 0) {
""")
            original = source.read_text()
            command = ["patch", "--batch", "--forward", "-d", str(root), "-p1", "-i", str(patch)]
            subprocess.run(command, check=True, capture_output=True)
            subprocess.run([*command, "--dry-run", "--reverse"], check=True, capture_output=True)
            patched = source.read_text()
            self.assertNotIn("iPAddress->data", patched)
            self.assertIn("ASN1_STRING_get0_data(name->d.iPAddress)", patched)
            self.assertIn("ASN1_STRING_length(name->d.iPAddress)", patched)
            opaque_api = '''#include <cstddef>
struct asn1_string_st;
using ASN1_STRING = asn1_string_st;
extern "C" const unsigned char* ASN1_STRING_get0_data(const ASN1_STRING*);
extern "C" int ASN1_STRING_length(const ASN1_STRING*);
struct GENERAL_NAME { union { ASN1_STRING* iPAddress; } d; };
int inspect(GENERAL_NAME* name) {
'''
            for content, valid in ((original, False), (patched, True)):
                with self.subTest(patched=valid):
                    access = content[content.index('      unsigned char'):content.index('\n\n      if')]
                    result = subprocess.run(
                        ['c++', '-std=c++20', '-Wall', '-Wextra', '-Werror', '-fsyntax-only', '-x', 'c++', '-'],
                        input=opaque_api + access + '\nreturn rawIpLen ? rawIpStr[0] : 0;\n}\n',
                        capture_output=True, text=True)
                    self.assertEqual(result.returncode == 0, valid, result.stderr)


if __name__ == "__main__":
    unittest.main()

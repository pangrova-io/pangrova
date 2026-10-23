#!/usr/bin/env python3
"""Exercise real Checkstyle diagnostics and blocking code-rule fixtures."""

from copy import deepcopy
from pathlib import Path
import subprocess
import tempfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[3]
CONFIG = ET.parse(ROOT / 'config/checkstyle/checkstyle.xml')
POM = ET.parse(ROOT / 'pom.xml')
NS = {'m': 'http://maven.apache.org/POM/4.0.0'}
PROPERTIES = POM.find('m:properties', NS)
PLUGIN_VERSION = PROPERTIES.find('m:maven-checkstyle-plugin.version', NS).text
ENGINE_VERSION = PROPERTIES.find('m:checkstyle.version', NS).text


def method_source(name, length):
    body = ['        int value = 0;'] + ['        value++;'] * (length - 4)
    body += ['        return value;']
    return '\n'.join([f'class {name} {{', '    int run() {', *body, '    }', '}']) + '\n'


def complexity_source(name, decisions):
    body = ['        if (value > 0) { value--; }'] * decisions
    return '\n'.join([f'class {name} {{', '    int run(int value) {', *body,
                      '        return value;', '    }', '}']) + '\n'


def parameters_source(name, count):
    params = ', '.join(f'int value{i}' for i in range(count))
    return f'class {name} {{\n    int run({params}) {{ return value0; }}\n}}\n'


def nested_source(name, statement, depth):
    closing = '} catch (RuntimeException e) { throw e; }' if statement == 'try' else '}'
    opening = 'try {' if statement == 'try' else 'if (value > 0) {'
    return '\n'.join([f'class {name} {{', '    void run(int value) {',
                      *[opening] * depth, 'value++;', *[closing] * depth, '    }', '}']) + '\n'


def main():
    checker = ET.Element('module', name='Checker')
    checker.append(deepcopy(CONFIG.find('./module[@name="FileLength"]')))
    walker = ET.SubElement(checker, 'module', name='TreeWalker')
    for name in ('MethodLength', 'ParameterNumber', 'CyclomaticComplexity',
                 'NestedIfDepth', 'NestedForDepth', 'NestedTryDepth', 'IllegalImport'):
        walker.append(deepcopy(CONFIG.find(f'.//module[@name="{name}"]')))
    expected = {'File801': 'FileLength', 'Method101': 'MethodLength',
                'Complexity16': 'CyclomaticComplexity', 'Parameters11': 'ParameterNumber',
                'DeepIf': 'NestedIfDepth', 'DeepFor': 'NestedForDepth', 'DeepTry': 'NestedTryDepth',
                'Constructor11': 'ParameterNumber'}
    imports = {
        'PublicHttpsApi': 'com.sun.net.httpserver.HttpsServer',
        'PublicHttpWildcard': 'com.sun.net.httpserver.*',
        'PublicHttpProvider': 'com.sun.net.httpserver.spi.HttpServerProvider',
        'InternalSun': 'sun.misc.Unsafe',
        'InternalComSun': 'com.sun.crypto.provider.SunJCE',
        'LookalikeHttpPrefix': 'com.sun.net.httpserverx.HttpsServer',
        'LookalikeHttpPackage': 'com.sun.net.http.server.HttpsServer',
        'InternalJdk': 'jdk.internal.misc.Unsafe',
        'StaticInternalSun': 'static sun.misc.Unsafe.getUnsafe',
    }
    expected.update({name: 'IllegalImport' for name in imports if not name.startswith('Public')})
    with tempfile.TemporaryDirectory(prefix='pangrova-checkstyle-') as directory:
        root = Path(directory)
        sources = root / 'src/main/java'
        sources.mkdir(parents=True)
        for name, imported in imports.items():
            (sources / f'{name}.java').write_text(f'import {imported};\nclass {name} {{}}\n')
        for length in (800, 801):
            (sources / f'File{length}.java').write_text(
                f'class File{length} {{\n' + '// fixture\n' * (length - 2) + '}\n')
        for length in (100, 101):
            (sources / f'Method{length}.java').write_text(method_source(f'Method{length}', length))
        for complexity in (15, 16):
            (sources / f'Complexity{complexity}.java').write_text(
                complexity_source(f'Complexity{complexity}', complexity - 1))
        for count in (10, 11):
            (sources / f'Parameters{count}.java').write_text(parameters_source(f'Parameters{count}', count))
        for count in (10, 11):
            params = ', '.join(f'int value{i}' for i in range(count))
            (sources / f'Constructor{count}.java').write_text(
                f'class Constructor{count} {{\n    Constructor{count}({params}) {{}}\n}}\n')
        for name, depth in (('ShallowFor', 4), ('DeepFor', 5)):
            loops = [f'for (int i{i} = 0; i{i} < value; i{i}++) {{' for i in range(depth)]
            (sources / f'{name}.java').write_text('\n'.join(
                [f'class {name} {{', 'void run(int value) {', *loops,
                 'value++;', *['}'] * depth, '}', '}']) + '\n')
        (sources / 'DeepIf.java').write_text(nested_source('DeepIf', 'if', 5))
        (sources / 'DeepTry.java').write_text(nested_source('DeepTry', 'try', 4))
        (root / 'checks.xml').write_text(
            '<?xml version="1.0"?>\n<!DOCTYPE module PUBLIC '
            '"-//Checkstyle//DTD Checkstyle Configuration 1.3//EN" '
            '"https://checkstyle.org/dtds/configuration_1_3.dtd">\n'
            + ET.tostring(checker, encoding='unicode'))
        (root / 'pom.xml').write_text(f'''<project>
  <modelVersion>4.0.0</modelVersion><groupId>gate.fixture</groupId>
  <artifactId>boundaries</artifactId><version>1</version>
  <build><plugins><plugin>
    <groupId>org.apache.maven.plugins</groupId><artifactId>maven-checkstyle-plugin</artifactId>
    <version>{PLUGIN_VERSION}</version>
    <dependencies><dependency><groupId>com.puppycrawl.tools</groupId>
      <artifactId>checkstyle</artifactId><version>{ENGINE_VERSION}</version>
    </dependency></dependencies>
    <configuration><configLocation>{root / 'checks.xml'}</configLocation>
      <violationSeverity>error</violationSeverity><failsOnError>true</failsOnError>
    </configuration>
  </plugin></plugins></build>
</project>''')
        result = subprocess.run(['mvn', '-B', '-q', 'checkstyle:check'], cwd=root,
                                capture_output=True, text=True)
        report = root / 'target/checkstyle-result.xml'
        if not report.exists():
            raise RuntimeError('Checkstyle produced no evidence:\n' + result.stdout + result.stderr)
        warnings = {"FileLength", "MethodLength", "CyclomaticComplexity", "ParameterNumber"}
        findings = {}
        for file in ET.parse(report).findall('file'):
            errors = {error.get('source').rsplit('.', 1)[-1].removesuffix('Check')
                      for error in file.findall('error')}
            for error in file.findall('error'):
                check = error.get('source').rsplit('.', 1)[-1].removesuffix('Check')
                assert error.get('severity') == ('warning' if check in warnings else 'error'), error.attrib
            if errors:
                findings[Path(file.get('name')).stem] = errors
        assert findings == {name: {check} for name, check in expected.items()}, findings
        assert result.returncode != 0, 'Blocking import/nesting fixtures must fail the real Maven gate'
        for name, check in expected.items():
            if check not in warnings:
                (sources / f'{name}.java').unlink()
        warnings_only = subprocess.run(['mvn', '-B', '-q', 'checkstyle:check'], cwd=root,
                                       capture_output=True, text=True)
        assert warnings_only.returncode == 0, warnings_only.stdout + warnings_only.stderr
        remaining = [error for file in ET.parse(report).findall('file') for error in file.findall('error')]
        assert len(remaining) == sum(check in warnings for check in expected.values()), remaining
        assert all(error.get('severity') == 'warning' for error in remaining), remaining
        print('PASS: boundaries retained as diagnostics; internal imports/nesting block; warnings-only build passes.')


if __name__ == '__main__':
    main()

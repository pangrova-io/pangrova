#!/usr/bin/env python3
"""Verify the exact immutable-hash classification with the pinned SpotBugs Filter API."""

import os
from copy import deepcopy
from pathlib import Path
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[3]
NS = {'m': 'http://maven.apache.org/POM/4.0.0'}

SOURCE = '''
import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.filter.Filter;

public final class ImmutableFilterBoundary {
    private static final String OWNER =
            "org.pangrova.protocol.function.declaration.FunctionBindingDeclarations";
    private static final String BYTES = "Lcom/google/protobuf/ByteString;";

    private static void check(Filter filter, String owner, String method, String parameters,
            String result, String field, String type, String bug, boolean expected) {
        BugInstance finding = new BugInstance(bug, 2).addClass(owner)
                .addMethod(owner, method, "(" + parameters + ")" + result, true)
                .addField(owner, field, type, false);
        if (filter.match(finding) != expected) {
            throw new AssertionError("Unexpected classification: " + owner + "." + method
                    + " result=" + result + " field=" + field + " type=" + type + " bug=" + bug);
        }
    }

    public static void main(String[] args) throws Exception {
        Filter filter = Filter.parseFilter(args[0]);
        check(filter, OWNER, "declarationSha256", "", BYTES, "declarationSha256", BYTES, "MS_EXPOSE_REP", true);
        check(filter, OWNER, "declarationSha256", "", "[B", "declarationSha256", "[B", "MS_EXPOSE_REP", false);
        check(filter, OWNER, "declarationSha256", "", "[B", "declarationSha256", BYTES, "MS_EXPOSE_REP", false);
        check(filter, OWNER, "declarationSha256", "", BYTES, "declarationSha256", "[B", "MS_EXPOSE_REP", false);
        check(filter, OWNER, "otherHash", "", BYTES, "declarationSha256", BYTES, "MS_EXPOSE_REP", false);
        check(filter, OWNER, "declarationSha256", "", BYTES, "otherHash", BYTES, "MS_EXPOSE_REP", false);
        check(filter, OWNER + "Other", "declarationSha256", "", BYTES, "declarationSha256", BYTES,
                "MS_EXPOSE_REP", false);
        check(filter, OWNER, "declarationSha256", "", BYTES, "declarationSha256", BYTES, "EI_EXPOSE_REP", false);
        check(filter, OWNER, "declarationSha256", "I", BYTES, "declarationSha256", BYTES, "MS_EXPOSE_REP", false);
    }
}
'''


def main():
    properties = ET.parse(ROOT / 'pom.xml').find('m:properties', NS)
    version = properties.find('m:spotbugs.version', NS).text
    repository = Path(os.environ.get('MAVEN_REPO_LOCAL', str(Path.home() / '.m2/repository')))
    engine = repository / 'com/github/spotbugs/spotbugs' / version
    plugin_version = properties.find('m:spotbugs-maven-plugin.version', NS).text
    plugin = ET.parse(repository / 'com/github/spotbugs/spotbugs-maven-plugin' / plugin_version
                      / f'spotbugs-maven-plugin-{plugin_version}.pom')
    plugin_properties = plugin.find('m:properties', NS)
    overrides = {}
    for dependency in plugin.findall('m:dependencies/m:dependency', NS):
        declared = dependency.find('m:version', NS)
        if declared is None:
            continue
        selected = declared.text
        if selected.startswith('${'):
            property_value = plugin_properties.find('m:' + selected[2:-1], NS)
            if property_value is None:
                continue
            selected = property_value.text
        overrides[(dependency.find('m:groupId', NS).text,
                   dependency.find('m:artifactId', NS).text)] = selected
    dependencies = [engine / f'spotbugs-{version}.jar']
    pom_files = [engine / f'spotbugs-{version}.pom',
                 repository / 'com/github/spotbugs/spotbugs-annotations' / version
                 / f'spotbugs-annotations-{version}.pom']
    for pom in pom_files:
        for dependency in ET.parse(pom).findall('m:dependencies/m:dependency', NS):
            group = dependency.find('m:groupId', NS).text
            artifact = dependency.find('m:artifactId', NS).text
            dependency_version = overrides.get((group, artifact), dependency.find('m:version', NS).text)
            dependencies.append(repository / group.replace('.', '/') / artifact / dependency_version
                                / f'{artifact}-{dependency_version}.jar')
    logger_version = overrides[('org.slf4j', 'slf4j-simple')]
    dependencies.append(repository / 'org/slf4j/slf4j-simple' / logger_version
                        / f'slf4j-simple-{logger_version}.jar')
    for dependency in dependencies:
        if not dependency.is_file():
            raise RuntimeError(f'Missing pinned SpotBugs dependency: {dependency}; run static-analysis first')
    java_home = os.environ.get('JAVA_HOME')
    javac = str(Path(java_home) / 'bin/javac') if java_home else 'javac'
    java = str(Path(java_home) / 'bin/java') if java_home else 'java'
    with tempfile.TemporaryDirectory(prefix='pangrova-immutable-filter-') as directory:
        temporary = Path(directory)
        # The distribution JAR's manifest names sibling files absent from Maven's
        # group/artifact layout. Use its unchanged classes/resources as a directory,
        # and the explicitly resolved dependencies, rather than suppress path lint.
        engine_classes = temporary / 'engine'
        with zipfile.ZipFile(dependencies[0]) as archive:
            archive.extractall(engine_classes)
        classpath = os.pathsep.join(map(str, [engine_classes, *dependencies[1:]]))
        full_filter = ET.parse(ROOT / 'config/spotbugs/exclude-filter.xml')
        matches = [match for match in full_filter.getroot()
                   if any(child.tag.endswith('Method') and child.get('name') == 'declarationSha256'
                          for child in match)
                   and any(child.tag.endswith('Field') and child.get('name') == 'declarationSha256'
                           for child in match)]
        if len(matches) != 1:
            raise RuntimeError('Expected one exact immutable hash classification')
        isolated = ET.Element('FindBugsFilter')
        isolated.append(deepcopy(matches[0]))
        for element in isolated.iter():
            element.tag = element.tag.rsplit('}', 1)[-1]
        classification = temporary / 'classification.xml'
        ET.ElementTree(isolated).write(classification, encoding='utf-8', xml_declaration=True)
        source = temporary / 'ImmutableFilterBoundary.java'
        source.write_text(SOURCE, encoding='utf-8')
        subprocess.run([javac, '-Xlint:all', '-Werror', '-cp', classpath,
                        '-d', str(temporary), str(source)], check=True)
        subprocess.run([java, '-cp', classpath + os.pathsep + str(temporary),
                        'ImmutableFilterBoundary', str(classification)], check=True)
    print('PASS: exact immutable hash classified; this Match rejects 8 changed-type/method/field/class/bug cases.')


if __name__ == '__main__':
    main()

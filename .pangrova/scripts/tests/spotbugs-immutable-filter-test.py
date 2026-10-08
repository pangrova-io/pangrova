#!/usr/bin/env python3
"""Verify narrow evidence-backed classifications with the pinned SpotBugs Filter API."""

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
import edu.umd.cs.findbugs.filter.SignatureUtil;
import edu.umd.cs.findbugs.Plugin;

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
        Plugin.loadCustomPlugin(new java.io.File(args[2]), null);
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
        Filter command = Filter.parseFilter(args[1]);
        String runner = "org.pangrova.testing.testkit.security.TestTlsAuthority";
        command(command, runner, "run", "(Ljava/util/List;)V", "COMMAND_INJECTION", true);
        command(command, runner + "Other", "run", "(Ljava/util/List;)V", "COMMAND_INJECTION", false);
        command(command, runner, "execute", "(Ljava/util/List;)V", "COMMAND_INJECTION", false);
        command(command, runner, "run", "(Ljava/lang/String;)V", "COMMAND_INJECTION", false);
        command(command, runner, "run", "()V", "COMMAND_INJECTION", false);
        command(command, runner, "run", "(Ljava/util/List;I)V", "COMMAND_INJECTION", false);
        command(command, runner, "run", "(Ljava/util/List;)Ljava/lang/Process;", "COMMAND_INJECTION", false);
        command(command, runner, "run", "(Ljava/util/List;)V", "OVERLY_PERMISSIVE_FILE_PERMISSION", false);
        Filter jitter = Filter.parseFilter(args[3]);
        String policy = "org.pangrova.client.rest.RetryPolicy";
        command(jitter, policy, "delayForAttemptWithJitter", "(I)Ljava/time/Duration;", "PREDICTABLE_RANDOM", true);
        command(jitter, policy + "Other", "delayForAttemptWithJitter", "(I)Ljava/time/Duration;",
                "PREDICTABLE_RANDOM", false);
        command(jitter, policy, "token", "(I)Ljava/time/Duration;", "PREDICTABLE_RANDOM", false);
        command(jitter, policy, "delayForAttemptWithJitter", "(J)Ljava/time/Duration;", "PREDICTABLE_RANDOM", false);
        command(jitter, policy, "delayForAttemptWithJitter", "()Ljava/time/Duration;", "PREDICTABLE_RANDOM", false);
        command(jitter, policy, "delayForAttemptWithJitter", "(II)Ljava/time/Duration;", "PREDICTABLE_RANDOM", false);
        command(jitter, policy, "delayForAttemptWithJitter", "(I)Ljava/lang/String;", "PREDICTABLE_RANDOM", false);
        command(jitter, policy, "delayForAttemptWithJitter", "(I)Ljava/time/Duration;", "HARD_CODE_KEY", false);
        for (String line : java.nio.file.Files.readAllLines(java.nio.file.Path.of(args[4]))) {
            String[] part = line.split("\\\\|", -1);
            Filter borrowed = Filter.parseFilter(part[0]);
            String owner = part[1], method = part[2];
            String signature = SignatureUtil.createMethodSignature(part[3], part[4]);
            int parameterEnd = signature.indexOf(')');
            String params = signature.substring(1, parameterEnd), result = signature.substring(parameterEnd + 1);
            String field = part[5], type = SignatureUtil.createFieldSignature(part[6]), bug = part[7];
            check(borrowed, owner, method, params, result, field, type, bug, true);
            check(borrowed, owner + "Other", method, params, result, field, type, bug, false);
            check(borrowed, owner, method + "Other", params, result, field, type, bug, false);
            check(borrowed, owner, method, params + "I", result, field, type, bug, false);
            check(borrowed, owner, method, "Ljava/lang/Object;", result, field, type, bug, false);
            check(borrowed, owner, method, params, "[B", field, type, bug, false);
            check(borrowed, owner, method, params, result, field + "Other", type, bug, false);
            check(borrowed, owner, method, params, result, field, "[B", bug, false);
            check(borrowed, owner, method, params, result, field, type,
                    bug.equals("EI_EXPOSE_REP") ? "EI_EXPOSE_REP2" : "EI_EXPOSE_REP", false);
            check(borrowed, owner, method, params, result, field, type, "HARD_CODE_KEY", false);
        }
        verifyJdbcForwards(args[5], args[6]);
        verifyReferenceBoundaries(args[7], args[6], "EI_EXPOSE_REP");
        verifyReferenceBoundaries(args[8], args[6], "EI_EXPOSE_REP");
        verifyReferenceBoundaries(args[9], args[6], "EI_EXPOSE_REP2");
        verifyMetadataDecisions(args[10], args[6]);
    }

    private static void verifyReferenceBoundaries(String manifest, String fullFilterPath, String bug) throws Exception {
        Filter full = Filter.parseFilter(fullFilterPath);
        for (String line : java.nio.file.Files.readAllLines(java.nio.file.Path.of(manifest))) {
            String[] part = line.split(java.util.regex.Pattern.quote("|"), -1);
            Filter isolated = Filter.parseFilter(part[0]);
            String owner = part[1], method = part[2], field = part[5];
            String signature = SignatureUtil.createMethodSignature(part[3], part[4]);
            int end = signature.indexOf(')');
            String params = signature.substring(1, end), result = signature.substring(end + 1);
            String type = SignatureUtil.createFieldSignature(part[6]);
            for (Filter filter : new Filter[]{isolated, full}) {
                check(filter, owner, method, params, result, field, type, bug, true);
                check(filter, owner + "Other", method, params, result, field, type, bug, false);
                check(filter, owner, method + "Other", params, result, field, type, bug, false);
                check(filter, owner, method, params + "I", result, field, type, bug, false);
                check(filter, owner, method, "Ljava/lang/Object;", result, field, type, bug, false);
                check(filter, owner, method, params, "[B", field, type, bug, false);
                check(filter, owner, method, params, "Ljava/lang/Object;", field, type, bug, false);
                check(filter, owner, method, params, result, field + "Other", type, bug, false);
                check(filter, owner, method, params, result, field, "[B", bug, false);
                check(filter, owner, method, params, result, field, "Ljava/lang/Object;", bug, false);
                check(filter, owner, method, params, "[B", field, "[B", bug, false);
                check(filter, owner, method, params, result, field, type,
                        bug.equals("EI_EXPOSE_REP") ? "EI_EXPOSE_REP2" : "EI_EXPOSE_REP", false);
                check(filter, owner, method, params, result, field, type, "MS_EXPOSE_REP", false);
                check(filter, owner, method, params, result, field, type, "NP_BOOLEAN_RETURN_NULL", false);
            }
        }
        String prepared = "org.pangrova.client.jdbc.PangrovaPreparedStatement";
        check(full, prepared, "getConnection", "", "Ljava/sql/Connection;", "connection",
                "Lorg/pangrova/client/jdbc/PangrovaConnection;", "EI_EXPOSE_REP", false);
        check(full, prepared, "getResultSet", "", "Ljava/sql/ResultSet;", "currentResultSet",
                "Ljava/sql/ResultSet;", "EI_EXPOSE_REP", false);
    }

    private static void verifyMetadataDecisions(String manifest, String fullFilterPath) throws Exception {
        Filter full = Filter.parseFilter(fullFilterPath);
        for (String line : java.nio.file.Files.readAllLines(java.nio.file.Path.of(manifest))) {
            String[] part = line.split(java.util.regex.Pattern.quote("|"), -1);
            Filter isolated = Filter.parseFilter(part[0]);
            String owner = part[1], method = part[2], params = part[3], result = part[4], bug = part[5];
            String signature = SignatureUtil.createMethodSignature(params, result);
            for (Filter filter : new Filter[]{isolated, full}) {
                command(filter, owner, method, signature, bug, true);
                command(filter, owner + "Other", method, signature, bug, false);
                command(filter, owner, method + "Other", signature, bug, false);
                command(filter, owner, method, SignatureUtil.createMethodSignature(params + ",int", result),
                        bug, false);
                command(filter, owner, method, SignatureUtil.createMethodSignature("java.lang.Object", result),
                        bug, false);
                command(filter, owner, method, SignatureUtil.createMethodSignature(params, "java.lang.Object"),
                        bug, false);
                command(filter, owner, method, signature,
                        bug.equals("UNSAFE_HASH_EQUALS") ? "PREDICTABLE_RANDOM" : "UNSAFE_HASH_EQUALS", false);
                command(filter, owner, method, signature, "HARD_CODE_KEY", false);
                command(filter, owner, "verifySensitiveBytes", "([BLjava/lang/String;)V",
                        "UNSAFE_HASH_EQUALS", false);
                command(filter, owner, "issueAuthenticationToken", "()Ljava/lang/String;",
                        "PREDICTABLE_RANDOM", false);
            }
        }
        String owner = "org.pangrova.metadata.service.DefaultMetadataService";
        String params = SignatureUtil.createMethodSignature(
                "org.pangrova.metadata.catalog.CatalogRegistry,org.pangrova.metadata.cache.MetadataCache,"
                + "org.pangrova.metadata.event.MetadataEventDispatcher,"
                + "org.pangrova.metadata.schema.SchemaEvolutionValidator,long,java.time.Duration", "void");
        check(full, owner, "<init>", params.substring(1, params.indexOf(')')), "V", "eventDispatcher",
                "Lorg/pangrova/metadata/event/MetadataEventDispatcher;", "EI_EXPOSE_REP2", true);
        String forwardingParams = SignatureUtil.createMethodSignature(
                "org.pangrova.metadata.catalog.CatalogRegistry,org.pangrova.metadata.cache.MetadataCache,"
                + "org.pangrova.metadata.event.MetadataEventDispatcher,"
                + "org.pangrova.metadata.schema.SchemaEvolutionValidator", "void");
        check(full, owner, "<init>", forwardingParams.substring(1, forwardingParams.indexOf(')')), "V",
                "eventDispatcher", "Lorg/pangrova/metadata/event/MetadataEventDispatcher;", "EI_EXPOSE_REP2", false);
        command(full, "org.pangrova.metadata.artifact.jdbc.JdbcArtifactStageExchangeStore",
                "requireProducerWinner", "(Ljava/sql/ResultSet;I)V", "UNSAFE_HASH_EQUALS", false);
        check(full, "org.pangrova.metadata.index.IndexSchemaEvolutionHandler", "<init>",
                "Lorg/pangrova/metadata/index/IndexMetadataStore;", "V", "metadataStore",
                "Lorg/pangrova/metadata/index/IndexMetadataStore;", "EI_EXPOSE_REP2", false);
        check(full, "org.pangrova.metadata.mv.JdbcMaterializedViewPersistenceHook", "<init>",
                "Ljavax/sql/DataSource;", "V", "dataSource", "Ljavax/sql/DataSource;", "EI_EXPOSE_REP2", false);
        check(full, "org.pangrova.metadata.cache.DdlPropagationWaiter", "<init>",
                "Lorg/pangrova/spi/coordination/CoordinatorRegistry;"
                + "Lorg/pangrova/metadata/cache/JdbcInvalidationPoller;Ljava/time/Duration;", "V", "localPoller",
                "Lorg/pangrova/metadata/cache/JdbcInvalidationPoller;", "EI_EXPOSE_REP2", false);
    }

    private static void verifyJdbcForwards(String manifest, String fullFilterPath) throws Exception {
        Filter full = Filter.parseFilter(fullFilterPath);
        for (String line : java.nio.file.Files.readAllLines(java.nio.file.Path.of(manifest))) {
            String[] part = line.split("\\\\|", -1);
            Filter isolated = Filter.parseFilter(part[0]);
            String owner = part[1], method = part[2], params = part[3], result = part[4];
            String signature = SignatureUtil.createMethodSignature(params, result);
            command(isolated, owner, method, signature, "SQL_INJECTION_JDBC", true);
            command(full, owner, method, signature, "SQL_INJECTION_JDBC", true);
            command(isolated, owner + "Other", method, signature, "SQL_INJECTION_JDBC", false);
            command(isolated, owner, method + "Other", signature, "SQL_INJECTION_JDBC", false);
            command(isolated, owner, method, SignatureUtil.createMethodSignature(params + ",int", result),
                    "SQL_INJECTION_JDBC", false);
            command(isolated, owner, method, SignatureUtil.createMethodSignature("java.lang.String", result),
                    "SQL_INJECTION_JDBC", false);
            command(isolated, owner, method, SignatureUtil.createMethodSignature(params, "java.lang.Object"),
                    "SQL_INJECTION_JDBC", false);
            command(isolated, owner, method, signature,
                    "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING", false);
            command(isolated, owner, method, signature, "NP_BOOLEAN_RETURN_NULL", false);
        }
        String prepared = "org.pangrova.client.jdbc.PangrovaPreparedStatement";
        command(full, prepared, "executeQuery", "()Ljava/sql/ResultSet;", "SQL_INJECTION_JDBC", false);
        command(full, "org.pangrova.client.jdbc.PangrovaStatement", "classifyTopLevelKeyword",
                "(Ljava/lang/String;I)Ljava/lang/Boolean;", "NP_BOOLEAN_RETURN_NULL", false);
    }

    private static void command(Filter filter, String owner, String method, String signature,
            String bug, boolean expected) {
        BugInstance finding = new BugInstance(bug, 2).addClass(owner).addMethod(owner, method, signature, false);
        if (filter.match(finding) != expected) {
            throw new AssertionError("Unexpected runner classification: " + owner + "." + method
                    + signature + " bug=" + bug);
        }
    }
}
'''


def exact_case_manifest(full_filter, expected, temporary, name, with_fields):
    owners = {case[0] for case in expected}
    bugs = {case[-1] for case in expected}
    matches = [match for match in full_filter.getroot()
               if any(child.tag.endswith('Class') and child.get('name') in owners for child in match)
               and any(child.tag.endswith('Bug') and child.get('pattern') in bugs for child in match)]
    if len(matches) != len(expected):
        raise RuntimeError(f'Expected {len(expected)} exact {name} classifications, found {len(matches)}')
    cases = []
    actual = set()
    required = {'Class', 'Method', 'Bug'} | ({'Field'} if with_fields else set())
    for index, match in enumerate(matches):
        elements = {child.tag.rsplit('}', 1)[-1]: child for child in match}
        if len(match) != len(required) or set(elements) != required:
            raise RuntimeError(f'{name} classification is not a flat exact Match')
        owner, method, bug = (elements[key] for key in ('Class', 'Method', 'Bug'))
        case = (owner.attrib['name'], method.attrib['name'], method.attrib['params'], method.attrib['returns'])
        if with_fields:
            field = elements['Field']
            case += (field.attrib['name'], field.attrib['type'])
        case += (bug.attrib['pattern'],)
        if case not in expected or case in actual:
            raise RuntimeError(f'Unexpected or duplicate {name} classification: {case}')
        actual.add(case)
        isolated = ET.Element('FindBugsFilter')
        isolated.append(deepcopy(match))
        for element in isolated.iter():
            element.tag = element.tag.rsplit('}', 1)[-1]
        path = temporary / f'{name}-{index}.xml'
        ET.ElementTree(isolated).write(path, encoding='utf-8', xml_declaration=True)
        cases.append('|'.join([str(path), *case]))
    manifest = temporary / f'{name}-cases.txt'
    manifest.write_text('\n'.join(cases), encoding='utf-8')
    return manifest


def main():
    properties = ET.parse(ROOT / 'pom.xml').find('m:properties', NS)
    version = properties.find('m:spotbugs.version', NS).text
    repository = Path(os.environ.get('MAVEN_REPO_LOCAL', str(Path.home() / '.m2/repository')))
    engine = repository / 'com/github/spotbugs/spotbugs' / version
    security_version = properties.find('m:findsecbugs.version', NS).text
    security_plugin = repository / 'com/h3xstream/findsecbugs/findsecbugs-plugin' / security_version \
        / f'findsecbugs-plugin-{security_version}.jar'
    if not security_plugin.is_file():
        raise RuntimeError(f'Missing pinned FindSecBugs plugin: {security_plugin}')
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
        runner_matches = [match for match in full_filter.getroot()
                          if any(child.tag.endswith('Class') and child.get('name') ==
                                 'org.pangrova.testing.testkit.security.TestTlsAuthority' for child in match)
                          and any(child.tag.endswith('Method') and child.get('name') == 'run' for child in match)]
        if len(runner_matches) != 1:
            raise RuntimeError('Expected one exact keytool runner classification')
        runner_filter = ET.Element('FindBugsFilter')
        runner_filter.append(deepcopy(runner_matches[0]))
        for element in runner_filter.iter():
            element.tag = element.tag.rsplit('}', 1)[-1]
        runner_classification = temporary / 'runner-classification.xml'
        ET.ElementTree(runner_filter).write(runner_classification, encoding='utf-8', xml_declaration=True)
        jitter_matches = [match for match in full_filter.getroot()
                          if any(child.tag.endswith('Class') and child.get('name') ==
                                 'org.pangrova.client.rest.RetryPolicy' for child in match)
                          and any(child.tag.endswith('Method') and child.get('name') ==
                                  'delayForAttemptWithJitter' for child in match)]
        if len(jitter_matches) != 1:
            raise RuntimeError('Expected one exact non-security retry jitter classification')
        jitter_filter = ET.Element('FindBugsFilter')
        jitter_filter.append(deepcopy(jitter_matches[0]))
        for element in jitter_filter.iter():
            element.tag = element.tag.rsplit('}', 1)[-1]
        jitter_classification = temporary / 'jitter-classification.xml'
        ET.ElementTree(jitter_filter).write(jitter_classification, encoding='utf-8', xml_declaration=True)
        borrowed_matches = [match for match in full_filter.getroot()
                            if any(child.tag.endswith('Class') and child.get('name', '').startswith(
                                'org.pangrova.connector.paimon.') for child in match)]
        if len(borrowed_matches) != 19:
            raise RuntimeError('Expected 19 exact borrowed Paimon service classifications')
        borrowed_cases = []
        for index, match in enumerate(borrowed_matches):
            elements = {child.tag.rsplit('}', 1)[-1]: child for child in match}
            if set(elements) != {'Class', 'Method', 'Field', 'Bug'}:
                raise RuntimeError('Borrowed service classification is not exact')
            owner, method, field, bug = (elements[key] for key in ('Class', 'Method', 'Field', 'Bug'))
            isolated_borrowed = ET.Element('FindBugsFilter')
            isolated_borrowed.append(deepcopy(match))
            for element in isolated_borrowed.iter():
                element.tag = element.tag.rsplit('}', 1)[-1]
            classification_file = temporary / f'borrowed-{index}.xml'
            ET.ElementTree(isolated_borrowed).write(classification_file, encoding='utf-8', xml_declaration=True)
            borrowed_cases.append('|'.join([str(classification_file), owner.attrib['name'], method.attrib['name'],
                                           method.attrib['params'], method.attrib['returns'], field.attrib['name'],
                                           field.attrib['type'], bug.attrib['pattern']]))
        borrowed_manifest = temporary / 'borrowed-cases.txt'
        borrowed_manifest.write_text('\n'.join(borrowed_cases), encoding='utf-8')
        jdbc_matches = [match for match in full_filter.getroot()
                        if any(child.tag.endswith('Class') and child.get('name', '').startswith(
                            'org.pangrova.client.jdbc.') for child in match)
                        and any(child.tag.endswith('Bug') and child.get('pattern') == 'SQL_INJECTION_JDBC'
                                for child in match)]
        expected_jdbc = {
            ('org.pangrova.client.jdbc.PangrovaConnection', 'prepareStatement', params, 'java.sql.PreparedStatement')
            for params in ('java.lang.String,int', 'java.lang.String,int,int', 'java.lang.String,int,int,int',
                           'java.lang.String,int[]', 'java.lang.String,java.lang.String[]')
        } | {
            ('org.pangrova.client.jdbc.PangrovaStatement', method, params, result)
            for method, result in (('execute', 'boolean'), ('executeUpdate', 'int'))
            for params in ('java.lang.String,int', 'java.lang.String,int[]', 'java.lang.String,java.lang.String[]')
        }
        if len(jdbc_matches) != len(expected_jdbc):
            raise RuntimeError('Expected 11 exact JDBC SQL forwarding classifications')
        jdbc_cases = []
        actual_jdbc = set()
        for index, match in enumerate(jdbc_matches):
            elements = {child.tag.rsplit('}', 1)[-1]: child for child in match}
            if len(match) != 3 or set(elements) != {'Class', 'Method', 'Bug'}:
                raise RuntimeError('JDBC forwarding classification is not a flat exact Match')
            owner, method = elements['Class'], elements['Method']
            case = (owner.attrib['name'], method.attrib['name'], method.attrib['params'], method.attrib['returns'])
            if case not in expected_jdbc or case in actual_jdbc:
                raise RuntimeError(f'Unexpected or duplicate JDBC classification: {case}')
            actual_jdbc.add(case)
            isolated_jdbc = ET.Element('FindBugsFilter')
            isolated_jdbc.append(deepcopy(match))
            for element in isolated_jdbc.iter():
                element.tag = element.tag.rsplit('}', 1)[-1]
            classification_file = temporary / f'jdbc-{index}.xml'
            ET.ElementTree(isolated_jdbc).write(classification_file, encoding='utf-8', xml_declaration=True)
            jdbc_cases.append('|'.join([str(classification_file), *case]))
        jdbc_manifest = temporary / 'jdbc-cases.txt'
        jdbc_manifest.write_text('\n'.join(jdbc_cases), encoding='utf-8')
        identity_matches = [match for match in full_filter.getroot()
                            if any(child.tag.endswith('Class') and child.get('name', '').startswith(
                                'org.pangrova.client.jdbc.') for child in match)
                            and any(child.tag.endswith('Bug') and child.get('pattern') == 'EI_EXPOSE_REP'
                                    for child in match)]
        expected_identities = {
            ('org.pangrova.client.jdbc.PangrovaDatabaseMetaData', 'getConnection', '', 'java.sql.Connection',
             'connection', 'org.pangrova.client.jdbc.PangrovaConnection'),
            ('org.pangrova.client.jdbc.PangrovaResultSet', 'getStatement', '', 'java.sql.Statement',
             'owningStatement', 'java.sql.Statement'),
            ('org.pangrova.client.jdbc.PangrovaStatement', 'getConnection', '', 'java.sql.Connection',
             'connection', 'org.pangrova.client.jdbc.PangrovaConnection'),
            ('org.pangrova.client.jdbc.PangrovaStatement', 'getResultSet', '', 'java.sql.ResultSet',
             'currentResultSet', 'java.sql.ResultSet'),
        }
        if len(identity_matches) != len(expected_identities):
            raise RuntimeError('Expected four exact JDBC identity getter classifications')
        identity_cases = []
        actual_identities = set()
        for index, match in enumerate(identity_matches):
            elements = {child.tag.rsplit('}', 1)[-1]: child for child in match}
            if len(match) != 4 or set(elements) != {'Class', 'Method', 'Field', 'Bug'}:
                raise RuntimeError('JDBC identity classification is not a flat exact Match')
            owner, method, field = (elements[key] for key in ('Class', 'Method', 'Field'))
            case = (owner.attrib['name'], method.attrib['name'], method.attrib['params'], method.attrib['returns'],
                    field.attrib['name'], field.attrib['type'])
            if case not in expected_identities or case in actual_identities:
                raise RuntimeError(f'Unexpected or duplicate JDBC identity classification: {case}')
            actual_identities.add(case)
            isolated_identity = ET.Element('FindBugsFilter')
            isolated_identity.append(deepcopy(match))
            for element in isolated_identity.iter():
                element.tag = element.tag.rsplit('}', 1)[-1]
            classification_file = temporary / f'jdbc-identity-{index}.xml'
            ET.ElementTree(isolated_identity).write(classification_file, encoding='utf-8', xml_declaration=True)
            identity_cases.append('|'.join([str(classification_file), *case]))
        identity_manifest = temporary / 'jdbc-identity-cases.txt'
        identity_manifest.write_text('\n'.join(identity_cases), encoding='utf-8')
        expected_metadata = {
            ('org.pangrova.metadata.artifact.build.ArtifactExchangeFinalization', 'partitions', 'java.util.List'),
            ('org.pangrova.metadata.artifact.build.ArtifactExchangeRegistration', 'endpoints', 'java.util.List'),
            ('org.pangrova.metadata.artifact.build.ArtifactStageGraph', 'stages', 'java.util.List'),
            ('org.pangrova.metadata.artifact.build.ArtifactStageGraph', 'exchanges', 'java.util.List'),
            ('org.pangrova.metadata.artifact.build.ArtifactStageGraph', 'modelSnapshots', 'java.util.List'),
            ('org.pangrova.metadata.artifact.build.template.ArtifactStageTemplate',
             'publicationReads', 'java.util.List'),
            ('org.pangrova.metadata.artifact.contract.ArtifactDependencySet', 'dependencies', 'java.util.List'),
            ('org.pangrova.metadata.model.TableMetadata', 'columns', 'java.util.List'),
            ('org.pangrova.metadata.model.TableMetadata', 'partitionSpec', 'java.util.Map'),
            ('org.pangrova.metadata.model.TableMetadata', 'sortOrder', 'java.util.List'),
            ('org.pangrova.metadata.model.TableMetadata', 'properties', 'java.util.Map'),
        }
        metadata_owners = {owner for owner, _, _ in expected_metadata}
        metadata_matches = [match for match in full_filter.getroot()
                            if any(child.tag.endswith('Class') and child.get('name') in metadata_owners
                                   for child in match)
                            and any(child.tag.endswith('Bug') and child.get('pattern') == 'EI_EXPOSE_REP'
                                    for child in match)]
        if len(metadata_matches) != len(expected_metadata):
            raise RuntimeError('Expected 11 exact immutable metadata collection classifications')
        metadata_cases = []
        actual_metadata = set()
        for index, match in enumerate(metadata_matches):
            elements = {child.tag.rsplit('}', 1)[-1]: child for child in match}
            if len(match) != 4 or set(elements) != {'Class', 'Method', 'Field', 'Bug'}:
                raise RuntimeError('Metadata collection classification is not a flat exact Match')
            owner, method, field = (elements[key] for key in ('Class', 'Method', 'Field'))
            case = (owner.attrib['name'], method.attrib['name'], method.attrib['returns'])
            if (case not in expected_metadata or case in actual_metadata or method.attrib['params'] != ''
                    or field.attrib['name'] != method.attrib['name']
                    or field.attrib['type'] != method.attrib['returns']):
                raise RuntimeError(f'Unexpected or duplicate metadata collection classification: {case}')
            actual_metadata.add(case)
            isolated_metadata = ET.Element('FindBugsFilter')
            isolated_metadata.append(deepcopy(match))
            for element in isolated_metadata.iter():
                element.tag = element.tag.rsplit('}', 1)[-1]
            classification_file = temporary / f'metadata-collection-{index}.xml'
            ET.ElementTree(isolated_metadata).write(classification_file, encoding='utf-8', xml_declaration=True)
            metadata_cases.append('|'.join([str(classification_file), case[0], case[1], '', case[2],
                                           field.attrib['name'], field.attrib['type']]))
        metadata_manifest = temporary / 'metadata-collection-cases.txt'
        metadata_manifest.write_text('\n'.join(metadata_cases), encoding='utf-8')
        metadata = 'org.pangrova.metadata.'
        store = metadata + 'store.MetadataStore'
        cache = metadata + 'cache.MetadataCache'
        dispatcher = metadata + 'event.MetadataEventDispatcher'
        registry = metadata + 'catalog.CatalogRegistry'
        registry_params = ','.join([store, metadata + 'catalog.ConnectorHandleFactory',
                                   metadata + 'catalog.FederationHandleFactory', dispatcher])
        service_params = ','.join([registry, cache, dispatcher, metadata + 'schema.SchemaEvolutionValidator',
                                  'long', 'java.time.Duration'])
        sync_params = ','.join([store, cache, dispatcher])
        expected_borrowed = {
            (metadata + 'cache.TieredMetadataCache', '<init>',
             'long,long,long,' + metadata + 'cache.L2CacheStore', 'void',
             'l2Adapter', metadata + 'cache.L2CacheStore', 'EI_EXPOSE_REP2'),
            (metadata + 'catalog.CatalogHealthMonitor', '<init>', 'java.util.Map', 'void',
             'catalogs', 'java.util.Map', 'EI_EXPOSE_REP2'),
            (registry, '<init>', registry_params, 'void', 'store', store, 'EI_EXPOSE_REP2'),
            (registry, '<init>', registry_params, 'void', 'eventDispatcher', dispatcher, 'EI_EXPOSE_REP2'),
            (metadata + 'service.DefaultMetadataService', '<init>', service_params, 'void',
             'catalogRegistry', registry, 'EI_EXPOSE_REP2'),
            (metadata + 'service.DefaultMetadataService', '<init>', service_params, 'void',
             'cache', cache, 'EI_EXPOSE_REP2'),
            (metadata + 'service.DefaultMetadataService', '<init>', service_params, 'void',
             'eventDispatcher', dispatcher, 'EI_EXPOSE_REP2'),
            (metadata + 'store.JdbcMetadataStore', '<init>', 'javax.sql.DataSource', 'void',
             'dataSource', 'javax.sql.DataSource', 'EI_EXPOSE_REP2'),
        } | {
            (metadata + 'sync.MetadataSyncService', '<init>', sync_params, 'void', field, field_type, 'EI_EXPOSE_REP2')
            for field, field_type in [('store', store), ('cache', cache), ('eventDispatcher', dispatcher)]
        }
        metadata_borrowed_manifest = exact_case_manifest(full_filter, expected_borrowed, temporary,
                                                        'metadata-borrowed', True)
        expected_decisions = {
            (metadata + 'artifact.jdbc.JdbcArtifactStageExchangeStore', 'requireGraphEdges',
             'java.sql.Connection,' + metadata + 'artifact.jdbc.CurrentDeferredExecutionContext',
             'void', 'UNSAFE_HASH_EQUALS'),
            (metadata + 'sync.MetadataSyncService', 'validateSyncedSample',
             'java.lang.String,' + metadata + 'adapter.FederatedCatalog,java.util.List',
             'void', 'PREDICTABLE_RANDOM'),
        }
        metadata_decisions_manifest = exact_case_manifest(full_filter, expected_decisions, temporary,
                                                         'metadata-decisions', False)
        source = temporary / 'ImmutableFilterBoundary.java'
        source.write_text(SOURCE, encoding='utf-8')
        subprocess.run([javac, '-Xlint:all', '-Werror', '-cp', classpath,
                        '-d', str(temporary), str(source)], check=True)
        subprocess.run([java, '-cp', classpath + os.pathsep + str(temporary),
                        'ImmutableFilterBoundary', str(classification), str(runner_classification),
                        str(security_plugin), str(jitter_classification), str(borrowed_manifest),
                        str(jdbc_manifest), str(ROOT / 'config/spotbugs/exclude-filter.xml'),
                        str(identity_manifest), str(metadata_manifest), str(metadata_borrowed_manifest),
                        str(metadata_decisions_manifest)], check=True)
    print('PASS: exact immutable hash classified; this Match rejects 8 changed-type/method/field/class/bug cases.')
    print('PASS: exact keytool runner classified; this Match rejects 7 class/method/signature/bug changes.')
    print('PASS: exact non-security retry jitter classified; this Match rejects 7 owner/method/signature/bug changes.')
    print('PASS: 19 exact borrowed Paimon services classified; each Match rejects 9 signature/field/type/bug changes.')
    print('PASS: 11 exact JDBC overloads classified; each Match rejects 7 owner/method/signature/bug changes.')
    print('PASS: complete filter does not exclude PreparedStatement SQL construction or Boolean-null finding.')
    print('PASS: four JDBC identity getters classified; isolated/full filters each reject 13 changed boundaries.')
    print('PASS: complete filter does not exclude obsolete PreparedStatement identity getter findings.')
    print('PASS: 11 immutable metadata collection getters classified; isolated/full filters reject changed boundaries.')
    print('PASS: 11 borrowed metadata constructors classified; isolated/full filters reject 13 changed boundaries.')
    print('PASS: public graph identity and non-security sampling classified with isolated/full negative controls.')
    print('PASS: forwarding Default constructor, three unwired constructors and sensitive bytes/tokens remain visible.')


if __name__ == '__main__':
    main()

#!/usr/bin/env bash
# Shared launcher classpath resolution. This file is sourced, never evals build output.
pangrova_runtime_classpath() {
    local role="$1" module artifact target properties version dependencies entry result=""
    local lib_dir="$PANGROVA_HOME/lib" jar
    # Role-specific distributions take precedence over the current shared distribution.
    [[ -d "$lib_dir/$role" ]] && lib_dir="$lib_dir/$role"
    if [[ -d "$lib_dir" ]]; then
        for jar in "$lib_dir"/*.jar; do
            [[ -f "$jar" ]] && result="${result:+$result:}$jar"
        done
        if [[ -n "$result" ]]; then
            printf '%s\n' "$result"
            return 0
        fi
    fi
    case "$role" in
        coordinator) module="pangrova-server/pangrova-server-coordinator"; artifact="pangrova-server-coordinator" ;;
        worker) module="pangrova-worker/pangrova-worker-core"; artifact="pangrova-worker-core" ;;
        standalone) module="pangrova-standalone"; artifact="pangrova-standalone" ;;
        *) echo "ERROR: Unknown runtime role: $role" >&2; return 1 ;;
    esac
    target="$PANGROVA_HOME/$module/target"
    properties="$target/maven-archiver/pom.properties"
    if [[ ! -f "$properties" || ! -s "$target/runtime-classpath.txt" ]]; then
        echo "ERROR: No packaged $role runtime. Run 'mvn -pl $module -am package -DskipTests' with the required runtime profile." >&2
        return 1
    fi
    version="$(sed -n 's/^version=//p' "$properties")"
    jar="$target/$artifact-$version.jar"
    if [[ -z "$version" || ! -f "$jar" ]]; then
        echo "ERROR: Missing $role application JAR; rebuild its package." >&2
        return 1
    fi
    dependencies="$(cat "$target/runtime-classpath.txt")"
    if [[ "$dependencies" == :* || "$dependencies" == *: || "$dependencies" == *::* ]]; then
        echo "ERROR: Empty runtime classpath entry; rebuild its package." >&2
        return 1
    fi
    local -a entries
    IFS=: read -r -a entries <<< "$dependencies"
    for entry in "${entries[@]}"; do
        if [[ ! -f "$entry" ]]; then
            echo "ERROR: A packaged runtime dependency is missing; rebuild its package." >&2
            return 1
        fi
    done
    printf '%s:%s\n' "$jar" "$dependencies"
}

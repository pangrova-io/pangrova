#!/usr/bin/env bash
# Shared launcher classpath resolution. This file is sourced, never evals build output.
pangrova_runtime_classpath() {
    local role="$1" module artifact target properties version dependencies entry result=""
    local lib_dir="$PANGROVA_HOME/lib" jar
    case "$role" in
        coordinator) module="pangrova-server/pangrova-server-coordinator"; artifact="pangrova-server-coordinator" ;;
        worker) module="pangrova-server/pangrova-server-worker"; artifact="pangrova-server-worker" ;;
        standalone) module="pangrova-standalone"; artifact="pangrova-standalone" ;;
        *) echo "ERROR: Unknown runtime role: $role" >&2; return 1 ;;
    esac
    lib_dir="$lib_dir/$role"
    if [[ -f "$lib_dir/classpath.list" ]]; then
        while IFS= read -r entry || [[ -n "$entry" ]]; do
            if [[ "$entry" != *.jar || "$entry" == */* || "$entry" == *:* || ! -f "$lib_dir/$entry" ]]; then
                echo "ERROR: Invalid $role runtime classpath entry: $entry" >&2
                return 1
            fi
            result="${result:+$result:}$lib_dir/$entry"
        done < "$lib_dir/classpath.list"
        [[ -n "$result" ]] || { echo "ERROR: Empty $role runtime classpath." >&2; return 1; }
        printf '%s\n' "$result"
        return 0
    fi
    if [[ -d "$lib_dir" ]]; then
        echo "ERROR: Missing ordered $role runtime classpath: $lib_dir/classpath.list" >&2
        return 1
    fi
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

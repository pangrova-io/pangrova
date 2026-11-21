# Paimon floating metadata text oracle

`jdk17.tsv` contains 40,405 finite, nonzero IEEE input/output records generated
by the actual JDK `Float.toString` and `Double.toString` methods. Expected text
does not come from the Native implementation. The generator covers every normal
exponent transition and three neighboring bit patterns on each side, both signs,
the first 2,047 subnormals, seeded random values, and explicit legacy rounding
boundaries. Native tests cover signed zero, NaN and infinities separately.

Generated with Amazon Corretto `17.0.20+8-LTS`, using:

```sh
java GenerateFloatingOracle.java jdk17.tsv
```

The generator requires JDK 17. The checked-in oracle requires no JVM or network
access when running Native tests. A separate, larger corpus generated from the
same JDK was used for 1,034,162 differential comparisons; the rolling validation
inventory records its evidence and distinguishes it from this checked-in corpus.

Paimon 1.4.1 `FilesTable.StatsLazyGetter` stores Java Float/Double objects in
TreeMaps whose strings use these methods. Its `Decimal.toString` uses
`BigDecimal.toPlainString`, which has separate Native regression checks.
Shortest-number printers are insufficient to reproduce the JDK 17 text contract:
the [OpenJDK explanation of the Java 19 change](https://inside.java/2022/09/23/quality-heads-up/)
describes observable differences, including `1e23`. The Native implementation
uses exact rational arithmetic and defined unsigned operations for legacy
fixed-width comparisons; it does not execute Java for each metadata value or
copy projected result rows into the query plan.

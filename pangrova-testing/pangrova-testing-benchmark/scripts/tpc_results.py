"""Typed, complete result comparison, including duplicates and legitimate empty results."""

from decimal import Decimal
from collections import Counter, deque
import math


def equal_cell(left, right, absolute=1e-8, relative=1e-6):
    if type(left) is not type(right):
        return False
    if isinstance(left, float):
        return math.isfinite(left) and math.isfinite(right) and math.isclose(
            left, right, abs_tol=absolute, rel_tol=relative)
    if isinstance(left, dict) and set(left) == {'decimal'} and set(right) == {'decimal'}:
        a, b = Decimal(left['decimal']), Decimal(right['decimal'])
        return a.is_finite() and b.is_finite() and a == b
    return left == right


def equal_row(left, right):
    return len(left) == len(right) and all(equal_cell(a, b) for a, b in zip(left, right))


def compare_result(expected, actual, order='ordered'):
    for result in (expected, actual):
        if not isinstance(result, dict) or not isinstance(result.get('columns'), list) or not isinstance(result.get('rows'), list):
            raise ValueError('missing complete reference/result rows and columns')
        if any(not isinstance(r, list) or len(r) != len(result['columns']) for r in result['rows']):
            raise ValueError('result row width differs from schema')
    # Native descriptors are retained separately; compare the declared logical family and names.
    if expected['columns'] != actual['columns']:
        raise ValueError('result column names/types differ')
    left, right = expected['rows'], actual['rows']
    if len(left) != len(right):
        raise ValueError(f'result row counts differ: {len(left)} != {len(right)}')
    if order == 'ordered':
        if not all(equal_row(a, b) for a, b in zip(left, right)):
            raise ValueError('ordered result values differ')
    elif order == 'unordered':
        # An exact multiset fast path preserves duplicates without quadratic work.
        def exact(value):
            if isinstance(value, dict) and set(value) == {'decimal'}:
                number = Decimal(value['decimal'])
                if not number.is_finite():
                    raise ValueError('non-finite decimal result')
                return ('decimal', number)
            if isinstance(value, dict):
                return tuple(sorted(value.items()))
            return (type(value).__name__, value)
        if Counter(tuple(exact(c) for c in row) for row in left) == Counter(tuple(exact(c) for c in row) for row in right):
            return
        # Iterative augmenting paths avoid greedy tolerance errors and recursion limits.
        matched_left, matched_right = {}, {}
        for root in range(len(left)):
            queue, parent, seen_left = deque([root]), {}, {root}
            free = None
            while queue and free is None:
                index = queue.popleft()
                for other, row in enumerate(right):
                    if other in parent or not equal_row(left[index], row):
                        continue
                    parent[other] = index
                    if other not in matched_right:
                        free = other
                        break
                    following = matched_right[other]
                    if following not in seen_left:
                        seen_left.add(following)
                        queue.append(following)
            if free is None:
                raise ValueError('unordered result values/multiplicities differ')
            while free is not None:
                index = parent[free]
                previous = matched_left.get(index)
                matched_left[index] = free
                matched_right[free] = index
                free = previous
    else:
        raise ValueError('order must be ordered or unordered')


def logical_type(native):
    kind = str(native).upper().split('(')[0].strip()
    if kind in ('TINYINT', 'SMALLINT', 'INTEGER', 'INT', 'BIGINT', 'LONG'):
        return 'integer'
    if kind in ('DOUBLE', 'REAL', 'FLOAT'):
        return 'float'
    if kind in ('DECIMAL', 'NUMERIC'):
        return 'decimal'
    if kind in ('VARCHAR', 'CHAR', 'STRING', 'TEXT'):
        return 'string'
    if kind in ('DATE', 'BOOLEAN'):
        return kind.lower()
    raise ValueError(f'unsupported benchmark result type: {kind}')


def cell(value, kind):
    if value is None:
        return None
    if kind == 'decimal':
        return {'decimal': str(value)}
    if kind == 'date':
        return {'date': str(value)}
    if kind == 'integer':
        return int(value)
    if kind == 'float':
        result = float(value)
        if not math.isfinite(result):
            raise ValueError('non-finite result value')
        return result
    if kind == 'boolean':
        return bool(value)
    return str(value)


def typed_result(names, types, rows):
    names, types, rows = list(names), list(types), list(rows)
    if len(names) != len(types) or any(len(row) != len(names) for row in rows):
        raise ValueError('result row width differs from schema')
    kinds = [logical_type(t) for t in types]
    return {'columns': [{'name': str(n).lower(), 'type': k} for n, k in zip(names, kinds)],
            'nativeTypes': [str(t) for t in types],
            'rows': [[cell(value, kind) for value, kind in zip(row, kinds)] for row in rows]}


def projection_names(sql):
    """Describe root projections without rewriting SQL or guessing expression names."""
    import re
    import sqlparse
    from sqlparse.sql import Identifier, Comment
    from sqlparse.tokens import DML, Keyword
    statements = sqlparse.parse(sql)
    if len(statements) != 1:
        raise ValueError('workload step must be one parsed statement')
    selected, fragments = False, []
    for token in statements[0].tokens:
        if token.ttype is DML and token.normalized == 'SELECT':
            selected = True
            continue
        if not selected or isinstance(token, Comment):
            continue
        if token.ttype is Keyword and token.normalized == 'FROM':
            break
        if not ''.join(fragments).strip() and token.ttype is Keyword and token.normalized in ('DISTINCT', 'ALL'):
            continue
        fragments.append(token.value)
    text = ''.join(fragments).strip()
    if not text:
        raise ValueError('benchmark step must be a relational SELECT')
    # Split projection commas at depth zero; literals/comments remain indivisible tokens.
    expressions, current, depth = [], [], 0
    for token in sqlparse.parse(text)[0].flatten():
        if token.value == ',' and depth == 0:
            expressions.append(''.join(current).strip())
            current = []
        else:
            current.append(token.value)
            if token.value == '(':
                depth += 1
            elif token.value == ')':
                depth -= 1
    expressions.append(''.join(current).strip())
    names = []
    for expression in expressions:
        if expression == '*' or re.fullmatch(r'\w+\.\*', expression):
            return None
        parsed = sqlparse.parse('SELECT ' + expression)[0]
        identifiers = [t for t in parsed.tokens if isinstance(t, Identifier)]
        alias = identifiers[0].get_alias() if len(identifiers) == 1 else None
        significant = [t for t in parsed.flatten() if not t.is_whitespace]
        depth = 0
        for index, token in enumerate(significant[:-1]):
            if token.value == '(':
                depth += 1
            elif token.value == ')':
                depth -= 1
            elif depth == 0 and token.normalized == 'AS':
                alias = significant[index + 1].value.strip('"`')
        simple = re.fullmatch(r'(?:[A-Za-z_]\w*\.)?([A-Za-z_]\w*)', expression)
        name = alias or (simple[1] if simple else None)
        names.append(name.lower() if name else None)
    return names


def normalize_projections(result, names):
    if names is None:
        return result
    if len(result['columns']) != len(names):
        raise ValueError('result projection count differs from workload')
    result['nativeColumnNames'] = [c['name'] for c in result['columns']]
    for index, (column, expected) in enumerate(zip(result['columns'], names)):
        if expected is not None and column['name'] != expected:
            raise ValueError('explicit projection name differs from workload')
        if expected is None:
            column['name'] = f'tpc_expression_{index + 1}'
    return result

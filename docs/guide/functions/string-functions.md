Pangrova > Guide > Functions > String Functions

# String Functions

---

## CONCAT / ||

```sql
CONCAT(str1, str2 [, ...]) → VARCHAR
str1 || str2 → VARCHAR
```

Concatenates two or more strings.

```sql
SELECT CONCAT(name, ' (', region, ')') AS display_name FROM users;
SELECT name || ' <' || email || '>' AS formatted FROM users;
```

---

## UPPER / LOWER

```sql
UPPER(str) → VARCHAR
LOWER(str) → VARCHAR
```

```sql
SELECT UPPER(name) AS upper_name, LOWER(email) AS lower_email FROM users;
```

---

## SUBSTRING

```sql
SUBSTRING(str FROM start FOR length) → VARCHAR
```

Extracts a substring. `start` is 1-based.

```sql
SELECT SUBSTRING(event_type FROM 1 FOR 5) AS prefix FROM events;
```

---

## TRIM / LTRIM / RTRIM

```sql
TRIM([LEADING|TRAILING|BOTH] [chars] FROM str) → VARCHAR
```

```sql
SELECT TRIM('  hello  ') AS trimmed;
SELECT TRIM(LEADING '0' FROM '000123') AS no_leading_zeros;
```

---

## CHAR_LENGTH / LENGTH

```sql
CHAR_LENGTH(str) → BIGINT
LENGTH(str) → BIGINT
```

Returns the number of characters in a string.

```sql
SELECT name, CHAR_LENGTH(name) AS name_length FROM users;
```

---

## LIKE

```sql
expr LIKE pattern → BOOLEAN
```

Pattern matching: `%` matches any sequence, `_` matches any single character.

```sql
SELECT * FROM users WHERE name LIKE 'A%';
SELECT * FROM users WHERE email LIKE '%@gmail.com';
SELECT * FROM products WHERE name LIKE 'Widget _';
```

---

## Notes

- All string functions handle NULL: if any argument is NULL, the result is NULL
- String comparisons are case-sensitive by default
- The `||` operator is equivalent to `CONCAT`

**See Also:** [Conditional Functions](conditional-functions.md), [SQL Reference](../sql-reference.md)

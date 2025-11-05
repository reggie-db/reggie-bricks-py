from pyspark.sql import functions as F
from pyspark.sql.column import Column


def infer_json_schema(c: Column | str) -> Column:
    """
    Create a schema string that describes the shape of a JSON string column.
    Returns array<struct<...>> when the value looks like a JSON array of objects. Returns struct<...> when the value looks like a JSON object. Returns variant for any other input.
    """
    col = _as_col(c)

    # Collect keys for array values then format each key as key variant
    array_keys = F.when(
        col.rlike(r"^\s*\["),
        F.array_sort(
            F.array_distinct(
                F.flatten(
                    F.transform(
                        F.from_json(col, "array<string>"),
                        lambda x: F.json_object_keys(x),
                    )
                )
            )
        ),
    )

    array_schema = F.when(
        F.coalesce(F.size(array_keys), F.lit(0)) > 0,
        F.concat(
            F.lit("array<struct<"),
            F.concat_ws(
                ", ", F.transform(array_keys, lambda k: F.concat(k, F.lit(" variant")))
            ),
            F.lit(">>"),
        ),
    )

    # Extract, sort, and format keys for object values
    object_keys = F.when(
        col.rlike(r"^\s*\{"),
        F.array_sort(F.json_object_keys(col)),
    )

    object_schema = F.when(
        F.coalesce(F.size(object_keys), F.lit(0)) > 0,
        F.concat(
            F.lit("struct<"),
            F.concat_ws(
                ", ", F.transform(object_keys, lambda k: F.concat(k, F.lit(" variant")))
            ),
            F.lit(">"),
        ),
    )

    return F.when(col.isNull(), F.lit(None)).otherwise(
        F.coalesce(array_schema, object_schema, F.lit("variant"))
    )


def infer_json_type(c: Column | str) -> Column:
    """
    Infer a simple JSON type name using the first significant character. Returns array, object, string, number, boolean, or null. Returns null when a type cannot be detected.
    """
    col = _as_col(c)
    return (
        F.when(col.isNull(), F.lit("null"))
        .when(col.rlike(r"^\s*\["), F.lit("array"))
        .when(col.rlike(r"^\s*\{"), F.lit("object"))
        .when(col.rlike(r'^\s*["\']'), F.lit("string"))
        .when(col.rlike(r"^\s*[+-]?[0-9]"), F.lit("number"))
        .when(col.rlike(r"^\s*[tT]"), F.lit("boolean"))
        .when(col.rlike(r"^\s*[fF]"), F.lit("boolean"))
        .when(col.rlike(r"^\s*[nN]"), F.lit("null"))
        .otherwise(F.lit("null"))
    )


def infer_json(
    c: Column | str,
    *,
    include_value: bool = True,
    include_schema: bool = True,
    include_type: bool = False,
) -> Column:
    """
    Build a JSON string that includes any requested fields among value, schema, and type. The schema is rendered as struct<value ...> where the inner part is inferred by infer_json_schema. The type is quoted when detected or null when unknown. Returns null when no fields are requested.
    """
    col = _as_col(c)
    if not (include_value or include_schema or include_type):
        return F.lit(None)

    # Accumulate fragments to avoid repeated string concatenations
    parts = [F.lit("{")]

    def _append(*frag_cols: Column) -> None:
        # Insert a comma when another field was already added
        if len(parts) > 1:
            parts.append(F.lit(","))
        parts.append(F.concat(*frag_cols))

    if include_value:
        # Write the raw column value without quoting
        _append(F.lit('"value":'), col)

    if include_schema:
        # Wrap the inferred schema under struct<value ...>
        _append(F.lit('"schema":"struct<value '), infer_json_schema(col), F.lit('>"'))

    if include_type:
        t = infer_json_type(col)
        # Quote the type string when present otherwise emit null
        t_json = F.when(t.isNull(), F.lit("null")).otherwise(
            F.concat(F.lit('"'), t, F.lit('"'))
        )
        _append(F.lit('"type":'), t_json)

    parts.append(F.lit("}"))
    return F.when(col.isNull(), F.lit(None)).otherwise(F.concat(*parts))


def infer_json_parse(c: Column | str) -> Column:
    """
    Parse the JSON string produced by infer_json into a struct using an external schema lookup.
    """
    col = _as_col(c)
    return F.from_json(infer_json(col), None, {"schemaLocationKey": "schema"})


def _as_col(c: Column | str) -> Column:
    """Return a Column whether input is a Column or a column name."""
    return c if isinstance(c, Column) else F.col(c)

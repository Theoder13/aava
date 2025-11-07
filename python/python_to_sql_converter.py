"""
python_to_sql_converter.py

A simple Python (pandas-style) -> SQL converter for common patterns:
- column access: df['col'] -> SELECT col FROM table;
- filtering: df[df['age'] > 30] -> SELECT * FROM table WHERE age > 30;
- chained: df[df['age'] > 30]['salary'].mean() -> SELECT AVG(salary) FROM table WHERE age > 30;
- groupby: df.groupby('dept')['salary'].sum() -> SELECT dept, SUM(salary) FROM table GROUP BY dept;
- sorting: df.sort_values('salary') -> SELECT * FROM table ORDER BY salary;

Usage:
    from python_to_sql_converter import python_to_sql
    sql = python_to_sql("df[(df['age'] > 30) & (df['city'] == 'Chennai')]['salary'].sum()", table_name="employees")
    print(sql)

CLI:
    python3 converter.py "df['salary'].mean()" --table sales
"""

import re
import argparse

FUNC_MAP = {
    "mean": "AVG",
    "sum": "SUM",
    "count": "COUNT",
    "min": "MIN",
    "max": "MAX",
    "median": "MEDIAN"
}

def _clean_quotes(s: str) -> str:
    # remove surrounding quotes if present
    s = s.strip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        return s[1:-1]
    return s

def _replace_series_refs(condition: str) -> str:
    """
    Replace pandas series references like df['col'] or df[\"col\"] inside condition
    with column names, and map python logical ops to SQL equivalents.
    Example: (df['age'] > 30) & (df['city'] == 'Chennai') -> (age > 30) AND (city = 'Chennai')
    """
    # replace df['col'] or df["col"] -> col
    condition = re.sub(r"df\[['\"](\w+)['\"]\]", r"\1", condition)
    # python equality and logical ops to SQL
    condition = condition.replace("==", "=")
    condition = condition.replace("&", "AND")
    condition = condition.replace("|", "OR")
    # remove redundant 'df.' if any
    condition = condition.replace("df.", "")
    return condition

def python_to_sql(python_code: str, table_name: str = "employees") -> str:
    code = python_code.strip()

    # 1) groupby pattern: df.groupby('gcol')['col'].agg()
    m = re.match(r"df\.groupby\(\s*['\"](\w+)['\"]\s*\)\s*\[\s*['\"](\w+)['\"]\s*\]\s*\.\s*(\w+)\s*\(\s*\)", code)
    if m:
        gcol, target_col, func = m.groups()
        sql_func = FUNC_MAP.get(func, func.upper())
        return f"SELECT {gcol}, {sql_func}({target_col}) FROM {table_name} GROUP BY {gcol};"

    # 2) sort_values: df.sort_values('col') or df.sort_values('col', ascending=False)
    m = re.match(r"df\.sort_values\(\s*['\"](\w+)['\"](?:\s*,\s*ascending\s*=\s*(True|False))?\s*\)", code)
    if m:
        col, asc = m.groups()
        order = "ASC"
        if asc == "False":
            order = "DESC"
        return f"SELECT * FROM {table_name} ORDER BY {col} {order};"

    # 3) chained filter + aggregate: df[...]['col'].func()
    m = re.match(r"df\[(.+)\]\s*\[\s*['\"](\w+)['\"]\s*\]\s*\.\s*(\w+)\s*\(\s*\)", code)
    if m:
        cond_raw, target_col, func = m.groups()
        cond = _replace_series_refs(cond_raw)
        sql_func = FUNC_MAP.get(func, func.upper())
        return f"SELECT {sql_func}({target_col}) FROM {table_name} WHERE {cond};"

    # 4) filter only: df[condition]
    m = re.match(r"df\[(.+)\]\s*$", code)
    if m:
        cond_raw = m.group(1)
        cond = _replace_series_refs(cond_raw)
        return f"SELECT * FROM {table_name} WHERE {cond};"

    # 5) column with aggregate: df['col'].func()
    m = re.match(r"df\[['\"](\w+)['\"]\]\s*\.\s*(\w+)\s*\(\s*\)", code)
    if m:
        col, func = m.groups()
        sql_func = FUNC_MAP.get(func, func.upper())
        return f"SELECT {sql_func}({col}) FROM {table_name};"

    # 6) simple column select: df['col']
    m = re.match(r"df\[['\"](\w+)['\"]\]\s*$", code)
    if m:
        col = m.group(1)
        return f"SELECT {col} FROM {table_name};"

    return "ERROR: Unsupported or unrecognized Python pattern. Please try a simpler expression."

def _cli():
    parser = argparse.ArgumentParser(description="Convert simple pandas-style Python expressions to SQL.")
    parser.add_argument("expression", type=str, help="Python expression to convert (quote it)")
    parser.add_argument("--table", "-t", type=str, default="employees", help="Target SQL table name")
    args = parser.parse_args()
    print(python_to_sql(args.expression, table_name=args.table))

if __name__ == "__main__":
    _cli()

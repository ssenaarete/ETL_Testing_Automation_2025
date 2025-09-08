import re
import pandas as pd

def extract_schema_from_sql(sql_file, output_excel="schema_details.xlsx", default_database=None):
    with open(sql_file, "r", encoding="utf-8") as f:
        sql_text = f.read()

    # try to detect DB name if present
    db_name = default_database
    m = re.search(r'CREATE\s+DATABASE\s*\[?([^\]\s;]+)\]?', sql_text, re.I)
    if not m:
        m = re.search(r'USE\s+\[?([^\]\s;]+)\]?', sql_text, re.I)
    if m:
        db_name = m.group(1)

    def clean_col_list(s):
        parts = [p.strip() for p in re.split(r',', s) if p.strip()]
        cleaned = []
        for p in parts:
            p2 = re.sub(r'[\[\]]', '', p)
            p2 = re.sub(r'\bASC\b|\bDESC\b', '', p2, flags=re.I).strip()
            cleaned.append(p2)
        return cleaned

    # 1) find CREATE TABLE blocks via balanced parentheses (robust)
    create_pattern = re.compile(r'CREATE\s+TABLE\s*\[([^\]]+)\]', re.I)
    tables = {}
    for m in create_pattern.finditer(sql_text):
        table_name = m.group(1)
        search_start = m.end()
        open_pos = sql_text.find('(', search_start)
        if open_pos == -1:
            continue
        depth = 0
        close_pos = None
        for i in range(open_pos, len(sql_text)):
            ch = sql_text[i]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    close_pos = i
                    break
        if close_pos is None:
            continue
        tables[table_name] = sql_text[open_pos:close_pos+1]

    # 2) parse columns + inline constraints
    col_regex = re.compile(r'^\s*\[([^\]]+)\]\s+([^\s,]+(?:\s*\([^)]+\))?)(.*)$', re.I)
    table_columns = {}
    table_pks = {}   # table -> [pk columns]
    table_fks = []   # (table, fk_cols_list, ref_table, ref_cols_list)

    for tbl, block in tables.items():
        cols = []
        pks = []
        fks = []
        inner = block.strip()[1:-1]
        for raw in inner.splitlines():
            line = raw.strip().rstrip(',').strip()
            if not line:
                continue
            if line.startswith('['):
                mcol = col_regex.match(line)
                if mcol:
                    colname = mcol.group(1).strip()
                    dtype = mcol.group(2).strip()
                    rest = mcol.group(3) or ""
                    nullable = None
                    if re.search(r'\bNOT\s+NULL\b', rest, re.I):
                        nullable = "NOT NULL"
                    elif re.search(r'\bNULL\b', rest, re.I):
                        nullable = "NULL"
                    cols.append((colname, dtype, nullable))
                    continue
            # inline PK (PRIMARY KEY or PK)
            pk_inline = re.search(r'(?:PRIMARY\s+KEY|PK)[\s\S]*?\(([^)]*?)\)', line, re.I)
            if pk_inline:
                pks.extend(clean_col_list(pk_inline.group(1)))
                continue
            # inline FK (FOREIGN KEY or FK)
            fk_inline = re.search(
                r'(?:FOREIGN\s+KEY|FK)\s*\(([^)]*?)\)\s*REFERENCES\s*\[?([^\]\s(]+)\]?\s*\(([^)]*?)\)',
                line, re.I
            )
            if fk_inline:
                fkcols = clean_col_list(fk_inline.group(1))
                reftbl = fk_inline.group(2)
                refcols = clean_col_list(fk_inline.group(3))
                fks.append((tbl, fkcols, reftbl, refcols))
                continue
            # CONSTRAINT ... PRIMARY KEY / FOREIGN KEY
            con_pk = re.search(r'CONSTRAINT\s+\[[^\]]+\]\s*(?:PRIMARY\s+KEY|PK)[\s\S]*?\(([^)]*?)\)', line, re.I)
            if con_pk:
                pks.extend(clean_col_list(con_pk.group(1)))
                continue
            con_fk = re.search(
                r'CONSTRAINT\s+\[[^\]]+\]\s*(?:FOREIGN\s+KEY|FK)\s*\(([^)]*?)\)\s*REFERENCES\s*\[?([^\]\s(]+)\]?\s*\(([^)]*?)\)',
                line, re.I
            )
            if con_fk:
                fkcols = clean_col_list(con_fk.group(1))
                reftbl = con_fk.group(2)
                refcols = clean_col_list(con_fk.group(3))
                fks.append((tbl, fkcols, reftbl, refcols))
                continue

        table_columns[tbl] = cols
        if pks:
            seen = []
            for c in pks:
                if c not in seen:
                    seen.append(c)
            table_pks[tbl] = seen
        for fk in fks:
            table_fks.append(fk)

    # 3) parse ALTER TABLE blocks across file for PK/FK
    alter_iter = re.finditer(r'ALTER\s+TABLE\s*\[([^\]]+)\]([\s\S]*?)(?=(ALTER\s+TABLE|CREATE\s+TABLE|COMMIT|\Z))', sql_text, re.I)
    for m in alter_iter:
        tbl = m.group(1)
        block = m.group(2)
        for pkm in re.finditer(r'(?:PRIMARY\s+KEY|PK)[\s\S]*?\(([^)]*?)\)', block, re.I):
            pkcols = clean_col_list(pkm.group(1))
            if tbl in table_pks:
                for c in pkcols:
                    if c not in table_pks[tbl]:
                        table_pks[tbl].append(c)
            else:
                table_pks[tbl] = pkcols
        for fkm in re.finditer(r'(?:FOREIGN\s+KEY|FK)\s*\(([^)]*?)\)\s*REFERENCES\s*\[?([^\]\s(]+)\]?\s*\(([^)]*?)\)', block, re.I):
            fkcols = clean_col_list(fkm.group(1))
            reftbl = fkm.group(2)
            refcols = clean_col_list(fkm.group(3))
            table_fks.append((tbl, fkcols, reftbl, refcols))

    # 4) build final dataframe
    records = []
    for tbl, cols in table_columns.items():
        for colname, dtype, nullable in cols:
            constraints = []
            if tbl in table_pks and colname in table_pks[tbl]:
                if len(table_pks[tbl]) > 1:
                    constraints.append(f"PRIMARY KEY (composite: {', '.join(table_pks[tbl])})")
                else:
                    constraints.append("PRIMARY KEY")
            for (fk_tbl, fk_cols, ref_tbl, ref_cols) in table_fks:
                if fk_tbl == tbl and colname in fk_cols:
                    if len(fk_cols) > 1:
                        constraints.append(f"FOREIGN KEY (composite: {', '.join(fk_cols)}) → {ref_tbl}({', '.join(ref_cols)})")
                    else:
                        constraints.append(f"FOREIGN KEY → {ref_tbl}({', '.join(ref_cols)})")
            records.append({
                "Database": db_name,
                "Table": tbl,
                "Column": colname,
                "DataType": dtype,
                "Nullable": nullable,
                "Constraints": ", ".join(constraints) if constraints else None
            })

    df = pd.DataFrame(records)
    df.to_excel(output_excel, index=False)
    return df

# Example usage:
if __name__ == "__main__":
    sql_path = r"C:\Users\ssen\Desktop\Data_2025\Learnings & Skill UP\Automation ETL Testing\QuickDBD-Free Diagram_1.sql"
    out_xlsx = r"C:\Users\ssen\Desktop\Data_2025\Learnings & Skill UP\Automation ETL Testing\schema_details.xlsx"
    df = extract_schema_from_sql(sql_path, out_xlsx, default_database="QuickDBD")
    print(df.to_string(index=False))

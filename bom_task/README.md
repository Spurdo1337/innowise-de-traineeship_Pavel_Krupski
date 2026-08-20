# BoM Explosion — FIN material production hierarchy

Explodes each FIN material's full production chain (`FIN -> PROD -> ... -> ADD/RM`) into one
row per `(material, component)` pair, aggregated to annual quantities. Two implementations in
one notebook: pandas and SQL (DuckDB `WITH RECURSIVE`, queryable as a PostgreSQL view too).

## Requirements

Python 3.9+.

## Run

```bash
pip install -r requirements.txt
jupyter notebook task2_bom_explosion.ipynb
```

Result: `bom_explosion_result.xlsx`.

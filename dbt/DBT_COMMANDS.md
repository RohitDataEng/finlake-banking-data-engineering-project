# Day 7 dbt Commands

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install dbt-snowflake
cd dbt\finlake_dbt
dbt debug
dbt build
dbt docs generate
dbt docs serve
```

Commit:

```powershell
git add .
git commit -m "Add dbt transformations and tests on Snowflake"
git push origin main
```

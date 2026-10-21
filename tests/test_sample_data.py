import duckdb

from scripts.make_sample_bronze import make_sample


def test_sample_bronze_is_readable(tmp_path):
    make_sample(tmp_path / "bronze", rows=500)
    n = duckdb.sql(
        f"select count(*) from read_parquet('{tmp_path}/bronze/**/*.parquet', hive_partitioning=true)"
    ).fetchall()[0][0]
    assert n >= 500

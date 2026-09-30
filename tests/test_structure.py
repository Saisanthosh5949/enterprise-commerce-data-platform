from pathlib import Path

def test_required_modules_exist():
    root=Path(__file__).parents[1]
    for p in ['src/common/spark_session.py','src/generators/generate_all.py','src/transformations/bronze_to_silver.py','src/transformations/build_gold.py']:
        assert (root/p).exists()

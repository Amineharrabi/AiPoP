import importlib


def test_import_core_scripts():
    modules = [
        'scripts.setup_duckdb',
        'scripts.load_warehouse',
        'scripts.compute_features',
        'scripts.compute_indices',
        'scripts.detect_bubble',
        'scripts.data_manager'
    ]

    for mod in modules:
        try:
            importlib.import_module(mod)
        except Exception as e:
            # If importing raises, test should fail with helpful message
            raise AssertionError(f"Failed to import {mod}: {e}")

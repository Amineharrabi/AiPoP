import importlib


def test_import_core_setup():
    modules = [
        'setup.setup_duckdb',
        'setup.load_warehouse',
        'setup.compute_features',
        'setup.compute_indices',
        'setup.detect_bubble',
        'setup.data_manager'
    ]

    for mod in modules:
        try:
            importlib.import_module(mod)
        except Exception as e:
            # If importing raises, test should fail with helpful message
            raise AssertionError(f"Failed to import {mod}: {e}")

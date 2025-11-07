import inspect
import setup.compute_features as cf


def test_compute_features_functions_exist():
    funcs = ['compute_hype_features', 'compute_reality_features', 'compute_combined_features']
    for f in funcs:
        assert hasattr(cf, f), f"Expected function {f} in compute_features.py"
        assert inspect.isfunction(getattr(cf, f))

import inspect
import setup.detect_bubble as db


def test_detect_bubble_api():
    assert hasattr(db, 'BubbleDetector'), 'BubbleDetector class missing in detect_bubble.py'
    cls = getattr(db, 'BubbleDetector')
    assert inspect.isclass(cls)
    # check some methods
    for m in ('compute_divergence', 'compute_acceleration', 'detect_bubbles'):
        assert hasattr(cls, m) or hasattr(cls(), m), f"BubbleDetector missing method {m}"

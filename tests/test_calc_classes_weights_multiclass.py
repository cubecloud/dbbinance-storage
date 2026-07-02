"""calc_classes_weights: multiclass-safe fix (complete weight dict via num_classes).

The old code inferred the class set from whatever labels were present in the passed slice
(np.unique); with 3+/quad targets a slice can miss a class, yielding an INCOMPLETE dict
(extreme: a single-label slice hardcoded exactly 2 classes). ``num_classes`` makes it always
return a COMPLETE dict over range(num_classes); ``num_classes=None`` keeps legacy behaviour.

Loads the module file directly (importlib) so the test needs no DB/SALT creds — importing the
dbbinance package would trigger the getpass key chain.
"""
import importlib.util
import math
import os

import numpy as np

_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "dbbinance", "fetcher", "datautils.py")
_spec = importlib.util.spec_from_file_location("dbb_datautils_under_test", _PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
calc_classes_weights = _mod.calc_classes_weights


# ----------------------------- legacy path (num_classes=None) -----------------------------

def test_legacy_multi_present_matches_balanced():
    classes, counts, weights = calc_classes_weights(np.array([0, 0, 0, 1, 2, 2]))
    assert classes == [0, 1, 2]
    assert counts == [3, 1, 2]
    assert math.isclose(weights[0], 6 / 9, rel_tol=1e-9)
    assert math.isclose(weights[1], 2.0, rel_tol=1e-9)
    assert math.isclose(weights[2], 1.0, rel_tol=1e-9)


def test_legacy_single_label_dummy_2class():
    classes, _, weights = calc_classes_weights(np.zeros(10, dtype=int))
    assert classes == [0, 1]
    assert weights == {0: 0.99, 1: 0.1}


# --------------------------- multiclass-safe path (num_classes) ---------------------------

def test_complete_dict_when_class_missing():
    classes, counts, weights = calc_classes_weights(np.array([0, 0, 0, 0, 2]), num_classes=3)
    assert classes == [0, 1, 2]
    assert counts == [4, 0, 1]
    assert set(weights.keys()) == {0, 1, 2}
    assert math.isclose(weights[0], 0.625, rel_tol=1e-9)
    assert math.isclose(weights[2], 2.5, rel_tol=1e-9)
    assert math.isclose(weights[1], 2.5, rel_tol=1e-9)          # absent -> max present weight


def test_single_label_complete_dict_quad():
    classes, counts, weights = calc_classes_weights(np.full(8, 2, dtype=int), num_classes=4)
    assert classes == [0, 1, 2, 3]
    assert counts == [0, 0, 8, 0]
    assert set(weights.keys()) == {0, 1, 2, 3}
    assert weights[2] == 0.1
    assert weights[0] == weights[1] == weights[3] == 0.99


def test_downstream_key_safety_no_keyerror():
    subset = np.array([0, 0, 2, 2])                             # class 1 absent from weight slice
    _, _, weights = calc_classes_weights(subset, num_classes=3)
    for class_id in np.array([0, 1, 2, 1, 0, 2]):              # full data has class 1
        _ = weights[int(class_id)]                             # must not raise KeyError

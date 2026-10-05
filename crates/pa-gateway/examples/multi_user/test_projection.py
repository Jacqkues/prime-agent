"""Run with python3 -m unittest discover -s crates/pa-gateway/examples/multi_user."""
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).with_name("projection.py").read_text()


class ProjectionTest(unittest.TestCase):
    def setUp(self):
        self.namespace = {"__name__": "__main__", "app": {}}

    def project(self):
        emitted = []
        repl = types.ModuleType("rlm.repl")
        repl.emit = emitted.append
        with patch.dict(sys.modules, {"rlm.repl": repl}):
            exec(SOURCE, self.namespace)
        return emitted[0]["application/vnd.prime-agent.application-state+json"]

    def test_live_definitions_aliases_replacement_and_deletion(self):
        exec('''
from json import dumps
def total(values, /, *, tax=2):
    """Sum application values."""
    return sum(values) + tax
async def refresh():
    """Refresh application data."""
    return app
def _private(): pass
alias = total
''', self.namespace)
        entry = {"name": "total", "signature": "(values, /, *, tax=Ellipsis)", "description": "Sum application values.", "kind": "function"}
        self.assertEqual(self.project()["catalog"], {
            "entries": [dict(entry, name="alias"), {"name": "refresh", "signature": "()", "description": "Refresh application data.", "kind": "async_function"}, entry],
            "total": 3, "truncated": False, "error": None,
        })
        exec('def total(value):\n    """New implementation."""\n    return value\ndel alias, refresh', self.namespace)
        self.assertEqual(self.project()["catalog"]["entries"], [dict(entry, signature="(value)", description="New implementation.")])
        exec('total = 42', self.namespace)
        self.assertEqual(self.project()["catalog"], {"entries": [], "total": 0, "truncated": False, "error": None})

    def test_partial_failure_and_invalid_app_keep_catalog(self):
        with self.assertRaises(RuntimeError):
            exec('def created_before_error():\n    return 42\nraise RuntimeError("failed after definition")', self.namespace)
        self.namespace["app"] = {"invalid_key": {object(): 1}}
        projection = self.project()
        self.assertEqual((projection["catalog"]["entries"][0]["name"], projection["state_error"]),
                         ("created_before_error", "Application state could not be projected: TypeError"))

    def test_default_repr_and_function_bodies_are_not_executed(self):
        exec('''
class Dangerous:
    def __repr__(self):
        raise AssertionError("must not execute repr")
def action(value: Dangerous() = Dangerous()) -> Dangerous():
    raise AssertionError("must not call functions")
''', self.namespace)
        self.assertEqual(self.project()["catalog"]["entries"], [{
            "name": "action", "signature": "(value=Ellipsis)", "description": "", "kind": "function",
        }])

    def test_large_catalog_is_bounded_and_explicitly_partial(self):
        for index in range(150):
            exec(f'def function_{index:03d}():\n    """' + "é" * 600 + '\n    """\n    pass', self.namespace)
        catalog = self.project()["catalog"]
        self.assertEqual((catalog["total"], catalog["truncated"], catalog["error"]), (150, True, None))
        self.assertLessEqual(len(catalog["entries"]), 128)
        self.assertLess(len(json.dumps(catalog, ensure_ascii=False).encode()), 50 * 1024)


if __name__ == "__main__":
    unittest.main()

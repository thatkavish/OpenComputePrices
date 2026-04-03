import argparse
import io
import unittest
from contextlib import redirect_stdout
from unittest import mock

import collect


class _BoomCollector:
    requires_api_key = False
    api_key_env_var = ""

    def run(self):
        raise RuntimeError("boom")


class CollectTests(unittest.TestCase):
    def test_resolve_collector_names_supports_browser_and_non_browser_filters(self):
        args = argparse.Namespace(
            sources=["aws", "coreweave", "runpod"],
            browser=False,
            no_auth_only=False,
            no_browser=True,
            skip=[],
        )
        self.assertEqual(collect.resolve_collector_names(args), ["aws", "runpod"])

        args.browser = True
        args.no_browser = False
        self.assertEqual(collect.resolve_collector_names(args), ["coreweave"])

        args.browser = False
        args.no_auth_only = True
        self.assertEqual(collect.resolve_collector_names(args), ["aws"])

    def test_main_exits_non_zero_when_all_collectors_fail(self):
        with mock.patch.dict(collect.COLLECTORS, {"boom": _BoomCollector}, clear=True), \
             mock.patch.object(collect, "NO_AUTH_COLLECTORS", []), \
             mock.patch.object(collect, "BROWSER_COLLECTORS", []), \
             mock.patch.object(collect, "API_KEY_COLLECTORS", []), \
             mock.patch("sys.argv", ["collect.py", "boom", "--no-unify"]), \
             redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as exc:
                collect.main()

        self.assertEqual(exc.exception.code, 1)

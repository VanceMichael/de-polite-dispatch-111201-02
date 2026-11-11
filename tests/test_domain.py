import json
from pathlib import Path
import unittest

from collector.domain import *


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(Path('fixtures/sample.json').read_text())

    def test_domain_behavior(self):
        self.assertEqual(collect(self.payload)[0]['key'], 'live-1')
        self.assertEqual(eligible([Task.parse(self.payload['tasks'][0])], 100), [])
        with self.assertRaises(ValueError):
            Task.parse(dict(self.payload['tasks'][0], cost=0))

    def test_deterministic_observations(self):
        self.assertEqual(collect(self.payload), collect(self.payload))

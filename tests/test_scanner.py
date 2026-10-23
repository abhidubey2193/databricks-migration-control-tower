import unittest
from scanner import assess, scan

class ScannerTests(unittest.TestCase):
    def test_scheduled_job_requires_review(self):
        self.assertEqual(assess({"schedule": {"cron": "x"}})["status"], "REVIEW")
    def test_simple_job_is_candidate(self):
        self.assertEqual(assess({"tasks": []})["status"], "CANDIDATE")
    def test_no_mutation_mode(self):
        self.assertEqual(scan({"jobs": []})["mode"], "read-only")

if __name__ == "__main__":
    unittest.main()

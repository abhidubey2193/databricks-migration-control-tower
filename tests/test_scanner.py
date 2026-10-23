import unittest
from scanner import assess, scan

class ScannerTests(unittest.TestCase):
    def test_scheduled_job_requires_review(self):
        self.assertEqual(assess({"schedule": {"cron": "x"}})["status"], "REVIEW")
    def test_simple_job_is_candidate(self):
        self.assertEqual(assess({"tasks": []})["status"], "CANDIDATE")
    def test_no_mutation_mode(self):
        self.assertEqual(scan({"jobs": []})["mode"], "read-only")
    def test_nullable_notebook_metadata(self):
        job = {"tasks": [{"task_key": "sql", "notebook_task": None}]}
        self.assertEqual(assess(job)["status"], "CANDIDATE")
    def test_dbfs_path_needs_review(self):
        job = {"tasks": [{"task_key": "legacy", "notebook_task": {"notebook_path": "/dbfs/old"}}]}
        self.assertEqual(assess(job)["status"], "REVIEW")

if __name__ == "__main__":
    unittest.main()

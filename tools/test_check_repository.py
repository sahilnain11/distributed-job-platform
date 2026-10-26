"""Offline checks for the publication guard; no real credentials or network calls."""
from pathlib import Path
import tempfile
import unittest

import check_repository as check


class PublicRepositoryChecks(unittest.TestCase):
    def test_local_environment_keys_and_build_artifacts_are_prohibited(self):
        for name in [".env", ".env.local", "target/report.json", "secret.key", "id_ed25519", "private.log"]:
            self.assertTrue(check.prohibited_path(name), name)

    def test_documented_examples_and_source_are_allowed(self):
        for name in [".env.example", "src/main/App.java", "docs/scaling.md", ".github/workflows/ci.yml"]:
            self.assertFalse(check.prohibited_path(name), name)

    def test_realistic_token_shape_is_flagged_without_returning_matched_value(self):
        token = "ghp_" + "a" * 36
        findings = check.content_findings("token=" + token)
        self.assertEqual(findings, [(1, "GitHub access token")])
        self.assertNotIn(token, str(findings))

    def test_private_key_and_personal_path_are_flagged(self):
        key_header = "-----BEGIN " + "RSA PRIVATE KEY-----"
        home_path = "/Users" + "/private-owner/project"
        self.assertEqual(check.content_findings(key_header + "\n" + home_path),
                         [(1, "private-key material"), (2, "personal absolute filesystem path")])

    def test_documented_local_password_and_sha_are_not_secret_claims(self):
        self.assertEqual(check.content_findings("DB_PASSWORD=local-development-only\nsha256:" + "a" * 64), [])

    def test_links_accept_existing_targets_and_reject_missing_ones(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "exists.md").write_text("# Exists")
            issues = check.local_link_errors(root, root / "README.md", "[good](exists.md) [bad](missing.md)")
            self.assertEqual(issues, ["missing local link target: missing.md"])

    def test_build_output_link_is_not_public_even_if_it_exists(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "target").mkdir()
            (root / "target/report.md").write_text("private raw output")
            issues = check.local_link_errors(root, root / "README.md", "[raw](target/report.md)")
            self.assertEqual(len(issues), 1)
            self.assertIn("unpublished", issues[0])

    def test_html_links_and_escaping_paths_are_checked(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            issues = check.local_link_errors(root, root / "view.html", '<a href="../outside.md">Bad</a><a href="https://example.com">Web</a>')
            self.assertEqual(issues, ["local link escapes checkout"])


if __name__ == "__main__":
    unittest.main()
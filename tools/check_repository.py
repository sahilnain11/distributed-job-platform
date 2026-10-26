#!/usr/bin/env python3
"""Check only this repository's tracked public files; never print matched secrets.

Focused heuristics, not a complete secret detector/security audit. Run after
staging intended files, and complement with human review/GitHub secret scanning.
"""
from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
IGNORED_PROBES = ["target/private-report.json", ".env", ".env.local", ".env.production",
                  "tools/__pycache__/module.pyc", ".venv/private.txt", "private.pem", "private.key", "app.log"]
PRIVATE_NAMES = {".env", ".npmrc", ".pypirc", "credentials.json", "id_rsa", "id_ed25519"}
PRIVATE_EXTENSIONS = {".pem", ".key", ".p12", ".pfx", ".jks", ".hprof", ".jfr", ".log", ".pyc"}
SECRET_PATTERNS = {
    "private-key material": re.compile(r"-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----"),
    "GitHub access token": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})\b"),
    "AWS access-key identifier": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "Slack access token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
    "credential-bearing remote URL": re.compile(r"https?://[^\s/:]+:[^\s/@]+@github\.com"),
}
MACHINE_PATH = re.compile(r"/(?:Users|home)/[^\s/]+/")


def prohibited_path(path):
    item = Path(path)
    return (any(part in {"target", ".git", ".venv", "__pycache__"} for part in item.parts)
            or item.name in PRIVATE_NAMES or item.suffix.lower() in PRIVATE_EXTENSIONS
            or item.name.startswith(".env") and item.name != ".env.example")


def content_findings(text):
    result = []
    for number, line in enumerate(text.splitlines(), 1):
        for category, pattern in SECRET_PATTERNS.items():
            if pattern.search(line):
                result.append((number, category))
        if MACHINE_PATH.search(line):
            result.append((number, "personal absolute filesystem path"))
    return result


class LocalLinks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.targets = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "a" and "href" in attributes:
            self.targets.append(attributes["href"])
        if tag in {"img", "script", "iframe"} and "src" in attributes:
            self.targets.append(attributes["src"])


def local_link_errors(root, file, text):
    root = root.resolve()  # macOS /var may alias /private/var for temporary checkouts.
    if file.suffix.lower() == ".md":
        # Strip fenced code examples: a pretend SQL/JSON link is not a document link.
        prose = re.sub(r"^```[^\n]*\n.*?^```\s*$", "", text, flags=re.M | re.S)
        targets = re.findall(r"!?\[[^\]]*\]\(([^)]+)\)", prose)
    elif file.suffix.lower() == ".html":
        parser = LocalLinks()
        parser.feed(text)
        targets = parser.targets
    else:
        return []
    errors = []
    for target in targets:
        parsed = urlsplit(target)
        if parsed.scheme or parsed.netloc or not parsed.path:
            continue
        destination = (file.parent / unquote(parsed.path)).resolve()
        if root != destination and root not in destination.parents:
            errors.append("local link escapes checkout")
        elif not destination.exists():
            errors.append("missing local link target: " + parsed.path)
        elif any(part in {"target", ".git", ".venv"} for part in destination.relative_to(root).parts):
            errors.append("link points at unpublished build/private output: " + parsed.path)
    return errors


def git(*args, input_text=None):
    return subprocess.run(["git", "-C", str(ROOT), *args], input=input_text, text=True,
                          capture_output=True, timeout=15, check=True).stdout


def main():
    top = Path(git("rev-parse", "--show-toplevel").strip()).resolve()
    if top != ROOT:
        raise RuntimeError("Expected an independent repository at the project root; refusing to inspect its parent worktree")
    paths = [value for value in git("ls-files", "--cached", "-z").split("\0") if value]
    if not paths:
        raise RuntimeError("No tracked public files; stage the intended project files before checking")
    ignored = set(git("check-ignore", "--stdin", input_text="\n".join(IGNORED_PROBES) + "\n").splitlines())
    errors = [f"ignore guard missing: {probe}" for probe in IGNORED_PROBES if probe not in ignored]
    for relative in paths:
        file = ROOT / relative
        if prohibited_path(relative):
            errors.append(relative + ": prohibited public path")
        if file.is_symlink():
            errors.append(relative + ": symlink requires manual public review")
            continue
        if not file.is_file():
            errors.append(relative + ": tracked path missing from working tree")
            continue
        if file.stat().st_size > 1_000_000:
            errors.append(relative + ": oversized public artifact; review build/raw data exclusion")
        try:
            text = file.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue  # Binary assets need human review; do not dump content.
        for line, category in content_findings(text):
            errors.append(f"{relative}:{line}: {category}")
        errors.extend(f"{relative}: {issue}" for issue in local_link_errors(ROOT, file, text))
    if errors:
        print("Public-file checks FAILED (matched values deliberately omitted):")
        print("\n".join("- " + error for error in errors))
        return 1
    print(f"Public-file checks passed: {len(paths)} tracked files; ignore probes, local links and focused secret/path checks.")
    print("Not a complete security audit or Git-history scan. Review credentials and publication permissions separately.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError) as failure:
        print("Repository check could not complete: " + type(failure).__name__)
        sys.exit(1)
"""Offline public-media checks; no Playwright, ffmpeg, platform or Docker needed."""
import json
from pathlib import Path
import struct
import unittest

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "docs/demo"


class IllustratedDemoTests(unittest.TestCase):
    def setUp(self):
        self.story = json.loads((DEMO / "storyboard.json").read_text())

    def test_story_is_one_minute_with_unique_readable_scenes(self):
        scenes = self.story["scenes"]
        self.assertEqual(len(scenes), 12)
        self.assertEqual(len({scene["id"] for scene in scenes}), 12)
        self.assertEqual(len(scenes) * self.story["seconds_per_scene"], 60)
        for scene in scenes:
            self.assertLessEqual(len(scene["caption"]), 330)
            self.assertIsInstance(scene["step"], int)
            self.assertGreaterEqual(scene["step"], 0)
            self.assertIn(scene["scenario"], {"success", "duplicate", "retry", "crash"})

    def test_scene_changes_are_labelled_as_separate_examples(self):
        previous = "success"
        for scene in self.story["scenes"]:
            if scene["scenario"] != previous:
                self.assertIn("NEW EXAMPLE", scene["chapter"])
            previous = scene["scenario"]

    def test_public_disclosure_does_not_claim_a_live_recording(self):
        self.assertIn("not a live execution", self.story["disclosure"])
        player = (DEMO / "index.html").read_text()
        self.assertIn("not a live execution", player)
        self.assertIn("controls playsinline", player)
        self.assertNotIn(" autoplay", player)

    def test_gif_has_expected_header_dimensions_and_public_size(self):
        data = (DEMO / "preview.gif").read_bytes()
        self.assertIn(data[:6], {b"GIF87a", b"GIF89a"})
        self.assertEqual(struct.unpack("<HH", data[6:10]), (800, 450))
        self.assertLess(len(data), 1_000_000)
        self.assertGreater(len(data), 1000)

    def test_mp4_is_an_actual_small_iso_media_file(self):
        data = (DEMO / "walkthrough.mp4").read_bytes()
        self.assertEqual(data[4:8], b"ftyp")
        self.assertIn(b"avc1", data)
        self.assertLess(len(data), 1_000_000)
        self.assertGreater(len(data), 1000)

    def test_transcript_covers_each_scene_and_limits(self):
        text = (DEMO / "README.md").read_text()
        self.assertEqual(text.count("| 00:"), 12)
        self.assertIn("01:00", text)
        self.assertIn("not a live", text)
        self.assertIn("external effects", text)
        self.assertIn("pause/seek", text)

    def test_renderer_and_player_do_not_submit_backend_requests(self):
        renderer = (ROOT / "tools/render_demo.cjs").read_text()
        player = (DEMO / "index.html").read_text()
        self.assertIn("route.abort()", renderer)
        self.assertIn("connect-src 'none'", player)
        for token in ["fetch(", "XMLHttpRequest(", "WebSocket(", "http://localhost:8080"]:
            self.assertNotIn(token, renderer + player)


if __name__ == "__main__":
    unittest.main()
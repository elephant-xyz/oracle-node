import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "opendoor"
    / "prepare_target_seeds.py"
)
SPEC = importlib.util.spec_from_file_location("prepare_target_seeds", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

INGEST_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "opendoor"
    / "local_ingest.py"
)
INGEST_SPEC = importlib.util.spec_from_file_location("local_ingest", INGEST_PATH)
assert INGEST_SPEC and INGEST_SPEC.loader
INGEST = importlib.util.module_from_spec(INGEST_SPEC)
INGEST_SPEC.loader.exec_module(INGEST)

IDENTITY_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "opendoor"
    / "identity.py"
)
IDENTITY_SPEC = importlib.util.spec_from_file_location("identity", IDENTITY_PATH)
assert IDENTITY_SPEC and IDENTITY_SPEC.loader
IDENTITY = importlib.util.module_from_spec(IDENTITY_SPEC)
IDENTITY_SPEC.loader.exec_module(IDENTITY)


class PrepareTargetSeedsTest(unittest.TestCase):
    def test_address_score_ignores_direction_and_suffix_position(self):
        self.assertEqual(
            MODULE.address_score(
                "10184 SHERMAN AVE N",
                "10184 N Sherman Avenue",
            ),
            100,
        )
        self.assertEqual(
            MODULE.address_score(
                "1720 W BATAVIA RD",
                "1720 W Batavia Road",
            ),
            100,
        )
        self.assertEqual(
            MODULE.address_score("1721 W BATAVIA RD", "1720 W Batavia Rd"),
            0,
        )

    def test_highlands_url_reorders_the_cama_identifier(self):
        self.assertEqual(
            MODULE._highlands_url({"PARCEL_ID": "C01332801000013271"}),
            "https://www.hcpao.org/Search/Parcel/28330101000013271C",
        )
        self.assertEqual(
            MODULE._highlands_url({"PARCEL_ID": "A23332806000D00150"}),
            "https://www.hcpao.org/Search/Parcel/28332306000D00150A",
        )
        self.assertEqual(MODULE._highlands_url({"PARCEL_ID": "invalid"}), "")

    def test_county_urls_use_reviewed_identifiers(self):
        self.assertEqual(
            MODULE._baker_url({"PARCEL_ID": "362S21005100380120"}),
            "https://bakerpa.com/propertydetails.php?parcel=362S21005100380120",
        )
        self.assertEqual(
            MODULE._st_lucie_url({"ALT_KEY": "20331"}),
            "https://apps.paslc.gov/rerecordcard/20331",
        )
        citrus_url = MODULE._citrus_url(
            {"ALT_KEY": "2496967", "ASMNT_YR": 2025}
        )
        self.assertIn("pin=2496967", citrus_url)
        self.assertIn("taxyr=2025", citrus_url)

    def test_pick_feature_requires_county_and_exact_street(self):
        features = [
            {
                "CO_NO": 19,
                "PARCEL_ID": "wrong-county-shape",
                "PHY_ADDR1": "1610 N DALARY PT",
            },
            {
                "CO_NO": 38,
                "PARCEL_ID": "C01332801000013271",
                "PHY_ADDR1": "1720 W BATAVIA RD",
            },
        ]
        feature, reason = MODULE.pick_feature(
            features,
            {"street": "1720 W Batavia Road"},
            38,
        )
        self.assertEqual(reason, "address_match")
        self.assertEqual(feature["PARCEL_ID"], "C01332801000013271")

    def test_local_ingest_supports_the_four_reviewed_counties(self):
        self.assertEqual(
            {
                county: INGEST.COUNTIES[county]["transform_dir"]
                for county in ("baker", "st-lucie", "highlands", "citrus")
            },
            {
                "baker": "baker",
                "st-lucie": "st. lucie",
                "highlands": "highlands",
                "citrus": "citrus",
            },
        )

    def test_capture_validation_requires_marker_and_exact_parcel(self):
        cfg = {
            "html_must_include": "Highlands County Property Appraiser",
        }
        INGEST.validate_capture(
            (
                "<title>Highlands County Property Appraiser</title>"
                "<h2>Parcel C-01-33-28-010-0001-3271</h2>"
            ),
            "https://example.test/property",
            cfg,
            "C01332801000013271",
        )
        with self.assertRaisesRegex(RuntimeError, "identifier missing"):
            INGEST.validate_capture(
                "<title>Highlands County Property Appraiser</title>",
                "https://example.test/property",
                cfg,
                "C01332801000013271",
            )
        with self.assertRaisesRegex(RuntimeError, "challenge"):
            INGEST.validate_capture(
                (
                    "<title>Highlands County Property Appraiser</title>"
                    "Parcel C01332801000013271 Just a moment"
                ),
                "https://example.test/property",
                cfg,
                "C01332801000013271",
            )

    def test_normalize_property_artifact_fills_required_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            parcel_dir = Path(directory)
            (parcel_dir / "data").mkdir()
            (parcel_dir / "data" / "property.json").write_text(
                json.dumps(
                    {
                        "request_identifier": None,
                        "parcel_identifier": "source-parcel",
                    }
                )
            )
            INGEST.normalize_property_artifact(
                parcel_dir,
                {
                    "parcel_id": "request-parcel",
                    "url": "https://example.test/request-parcel",
                },
                {"display": "Baker"},
                {
                    "elephant_uuid": "00000000-0000-5000-8000-000000000000",
                    "elephant_token": "a" * 64,
                },
            )
            payload = json.loads(
                (parcel_dir / "data" / "property.json").read_text()
            )
            self.assertEqual(payload["request_identifier"], "request-parcel")
            self.assertEqual(payload["parcel_identifier"], "source-parcel")
            self.assertEqual(payload["county_name"], "Baker")
            self.assertEqual(payload["elephant_token"], "a" * 64)

    def test_identity_validation_includes_the_open_door_unit(self):
        identity = IDENTITY.identity_from_opendoor(
            {
                "street": "1538 N Lawnwood Cir",
                "unit": "Apt 3",
                "postal_code": "34950",
                "state": "FL",
                "elephant_uuid": "4a184595-7b26-5e91-8071-1009133f5851",
                "elephant_token": (
                    "address:v1:"
                    "53fa246d170fc59f278d878ab24b28bb24641548bd68b1d182a1b16a8490bbdb"
                ),
            }
        )
        self.assertTrue(identity["mint_agrees"])
        self.assertEqual(identity["opendoor_unit"], "Apt 3")

    def test_completion_manifest_detects_artifact_changes(self):
        with tempfile.TemporaryDirectory() as temp:
            parcel_dir = Path(temp)
            artifact = parcel_dir / "input.html"
            artifact.write_text("captured")
            row = {"parcel_id": "123", "url": "https://example.test/123"}
            cfg = INGEST.COUNTIES["baker"]

            INGEST.write_completion_manifest(parcel_dir, row, cfg)
            self.assertTrue(
                INGEST.completion_manifest_valid(parcel_dir, row, cfg)
            )

            artifact.write_text("changed")
            self.assertFalse(
                INGEST.completion_manifest_valid(parcel_dir, row, cfg)
            )

    def test_parent_form_parser_ignores_other_forms(self):
        parser = INGEST._ParentFormParser()
        parser.feed(
            """
            <form id="other"><input name="tempPIN" value="wrong"></form>
            <form id="parentForm">
              <input name="tempPIN" value="">
              <input name="SearchResults_File" value="session-key">
            </form>
            """
        )
        self.assertEqual(
            parser.fields,
            {"tempPIN": "", "SearchResults_File": "session-key"},
        )


if __name__ == "__main__":
    unittest.main()

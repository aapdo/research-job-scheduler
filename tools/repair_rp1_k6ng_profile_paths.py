"""Create an immutable RP1 K6NG input-profile revision with node-local paths."""

import hashlib
import json
import shutil
from pathlib import Path


SOURCE = Path("/workspace/datasets/i3-k6-full-v1/INPUTS.json")
DESTINATION = Path("/workspace/datasets/i3-k6-full-v2-rp1")
W0 = Path(
    "/home/jy/carla_data/software/releases/"
    "bootstrap_k6_nominal_gate_v6_20260919/parent/payload/picodet_s_w0"
)
SOURCE_SHA = "1e868d99cb1d249201676d3a1731b45d317a8693d22fa4d4bcea3c50a0c9662c"
W0_SHA = "e646cdf7c22d61ed9487ee13ad51f04dabcc97d0fc899745aec27350cd3b7272"
COMMON_PLAN_SHA = "88469e7c09cedd61286e50f38156f982a3ce872381caf33e9f3b906fa17d7ea7"
COMMON_FILES = {
    "BASIS_READY.json": "7f23a09a37e5259ed50c9911b8696017e1462f0a95b6075af90fa933cddccbf7",
    "BOOTSTRAP.pdparams": "e0f8f5a005863b8b53f766469afd727c718c7dc1cb6a41c23f7e2e0230db901f",
    "BOOTSTRAP_RESULT.json": "2c1e546c6e7111bda42f178a9d21531cf00b04b2699df2ac709c632e4cd8c007",
    "NORMALIZATION.json": "fddf626e23b1796d68b4f083a3f2c7d442c9934b70c2d462b933844202a65c5b",
    "SUITE_BASIS.npz": "e103fe69369c265879787b4f2b4355e8249586fc7180f2e8f0fab45972f48bac",
}


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def main():
    assert sha(SOURCE) == SOURCE_SHA
    checkpoint = W0 / "train/best_model.pdparams"
    assert checkpoint.is_file() and sha(checkpoint) == W0_SHA
    profile = json.loads(SOURCE.read_text())
    assert profile["w0_sha256"] == W0_SHA
    assert profile["data"].startswith("/workspace/carla_data/")
    profile["w0"] = str(W0)
    runpod_profile = dict(profile.get("runpod_profile", {}))
    # This RP1 revision is independently prepared and verified; do not inherit
    # the older RP2/RP3 replica-equivalence claim.
    runpod_profile.pop("replicas_verified_equal", None)
    profile["runpod_profile"] = {
        **runpod_profile,
        "schema": "runpod-full-profile-v2-node-path-binding",
        "source_input_sha256": SOURCE_SHA,
        "node": "rp1",
        "path_binding": "rp1-local-release-and-dataset-roots",
    }
    DESTINATION.mkdir(parents=True, exist_ok=True)
    target = DESTINATION / "INPUTS.json"
    write_json(target, profile)
    profile_sha = sha(target)
    destination_common = DESTINATION / "published/s_cda_suite_v2"
    source_common = SOURCE.parent / "published/s_cda_suite_v2"
    if not source_common.is_dir():
        # A completed node may already have the verified common bundle only in
        # the corrected immutable profile. Keep reruns idempotent.
        source_common = destination_common
    ready = json.loads((source_common / "READY.json").read_text())
    assert ready["status"] == "complete" and ready["plan_sha256"] == COMMON_PLAN_SHA
    destination_common.mkdir(parents=True, exist_ok=True)
    for name, expected in COMMON_FILES.items():
        assert sha(source_common / name) == expected == ready["files"][name]
        destination = destination_common / name
        if not destination.exists():
            shutil.copyfile(source_common / name, destination)
        assert sha(destination) == expected
    ready_destination = destination_common / "READY.json"
    if not ready_destination.exists():
        shutil.copyfile(source_common / "READY.json", ready_destination)
    assert json.loads(ready_destination.read_text()) == ready
    manifest = {
        "schema": "runpod-full-profile-v2-node-path-binding",
        "node": "rp1",
        "source_profile_sha256": SOURCE_SHA,
        "profile_sha256": profile_sha,
        "files": {"INPUTS.json": {"bytes": target.stat().st_size, "sha256": profile_sha}},
        "w0": str(W0),
        "w0_sha256": W0_SHA,
        "common_plan_sha256": COMMON_PLAN_SHA,
        "common_files": COMMON_FILES,
    }
    write_json(DESTINATION / "COPY_MANIFEST.json", manifest)
    write_json(
        DESTINATION / "PROFILE.json",
        {
            "status": "prepared",
            "node": "rp1",
            "profile_sha256": profile_sha,
            "source_profile_sha256": SOURCE_SHA,
            "w0_sha256": W0_SHA,
        },
    )
    receipt = {**manifest, "status": "verified", "profile_path": str(DESTINATION)}
    write_json(Path("/workspace/RP1_K6NG_PROFILE_V2_READY.json"), receipt)
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    main()

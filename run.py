import argparse
import datetime
import json
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

# Import helper functions from Agent A to resolve directory conflict
# (Assumes agents/ directory is in python path or relative)
try:
    from agents.agent_a_intake import classify_file, extract_metadata_candidates, build_evidence_index_entry, compute_risk_indicators, _should_ignore
except ImportError:
    # Fallback if running from root without package install
    sys.path.append(str(Path(__file__).parent / "agents"))
    from agents.agent_a_intake import classify_file, extract_metadata_candidates, build_evidence_index_entry, compute_risk_indicators, _should_ignore


def setup_run_directory(bundle_path: Path, runs_root: Path) -> Path:
    """
    Creates the run directory and copies all necessary inputs (shared + bundle) into it.
    Mirrors Agent A's flattening logic but controls the directory name.
    """
    scenario_name = bundle_path.name
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = f"{scenario_name}_{timestamp}"
    run_dir = runs_root / run_id

    print(f"[Run Manager] Initializing Run: {run_id}")
    run_dir.mkdir(parents=True, exist_ok=True)

    # 1. Copy Shared Files
    shared_dir = bundle_path.parent / "shared"
    if shared_dir.exists():
        for item in shared_dir.iterdir():
            if item.is_file():
                shutil.copy2(item, run_dir / item.name)

    # 2. Copy Bundle Files (Overrides shared if name collision)
    if bundle_path.exists():
        for item in bundle_path.iterdir():
            if item.is_file():
                shutil.copy2(item, run_dir / item.name)
    else:
        print(f"Error: Bundle directory not found: {bundle_path}")
        sys.exit(1)

    return run_dir, run_id


def execute_agent_a_logic(run_dir: Path, run_id: Path, original_bundle_path: Path):
    """
    Executes Agent A's logic explicitly within run.py to avoid
    Agent A creating a separate, mismatched run directory.
    """
    print(f"[Agent A] Intake (In-Process Execution)...")

    context_packet = {
        "run_id": str(run_id),
        "timestamp": datetime.datetime.now().isoformat(),
        "status": "intake_complete",
        "files": [],
        "evidence_index": {},
        "risk_indicators": [],
        "metadata_candidates": {
            "vendor_ids": [],
            "po_refs": []
        },
        "system_paths": {
            "input_bundle": str(original_bundle_path.resolve()),
            "run_directory": str(run_dir.resolve()),
            "shared_config": str(original_bundle_path.parent / "shared")
        }
    }

    # Scan the populated run_dir to build context
    for item in run_dir.iterdir():
        if item.is_file() and not _should_ignore(item.name):
            # Classify
            file_type = classify_file(item.name)

            # Extract Metadata Candidates
            meta = extract_metadata_candidates(item)
            context_packet["metadata_candidates"]["vendor_ids"].extend(meta["potential_vendor_ids"])
            context_packet["metadata_candidates"]["po_refs"].extend(meta["potential_po_refs"])

            context_packet["files"].append({
                "filename": item.name,
                "type": file_type,
                "source": "run_dir_aggregated",
                "path": str(item.resolve())
            })
            context_packet["evidence_index"][item.name] = build_evidence_index_entry(item, file_type, "run_dir_aggregated")

    # Deduplicate metadata
    context_packet["metadata_candidates"]["vendor_ids"] = list(set(context_packet["metadata_candidates"]["vendor_ids"]))
    context_packet["metadata_candidates"]["po_refs"] = list(set(context_packet["metadata_candidates"]["po_refs"]))
    context_packet["risk_indicators"] = compute_risk_indicators(
        context_packet["files"], context_packet["metadata_candidates"]["vendor_ids"], run_dir
    )

    # Save Context Packet
    out_path = run_dir / "context_packet.json"
    out_path.write_text(json.dumps(context_packet, indent=4, ensure_ascii=False), encoding="utf-8")
    print(f"   -> Context saved: {out_path.name}")


def run_pipeline(bundle_dir_str: str) -> dict:
    """Run the full pipeline and return result dict with run_id, run_dir, action, and payload."""
    root_dir = Path(__file__).parent.resolve()
    runs_root = root_dir / "runs"
    bundle_path = Path(bundle_dir_str).resolve()

    # 1. Setup & Intake (Agent A equivalent)
    run_dir, run_id = setup_run_directory(bundle_path, runs_root)
    execute_agent_a_logic(run_dir, run_id, bundle_path)

    # 2. Define Sequential Pipeline (Agents B -> I)
    pipeline_steps = [
        {
            "name": "Agent B (Extraction)",
            "script": "agents/agent_b_extraction.py",
            "args": ["--bundle-dir", str(run_dir), "--run-dir", str(run_dir)]
        },
        {
            "name": "Agent C (Vendor Resolution)",
            "script": "agents/agent_c_vendor_resolution.py",
            "args": ["--bundle-dir", str(run_dir), "--out-dir", str(run_dir)]
        },
        {
            "name": "Agent D (Validation)",
            "script": "agents/agent_d_validation.py",
            "args": ["--bundle-dir", str(run_dir), "--run-dir", str(run_dir), "--out-dir", str(run_dir)]
        },
        {
            "name": "Agent E (Matching)",
            "script": "agents/agent_e_matching.py",
            "args": ["--bundle-dir", str(run_dir), "--out-dir", str(run_dir)]
        },
        {
            "name": "Agent F (Compliance)",
            "script": "agents/agent_f_compliance.py",
            "args": ["--bundle-dir", str(run_dir), "--run-dir", str(run_dir), "--out-dir", str(run_dir)]
        },
        {
            "name": "Agent G (Anomaly)",
            "script": "agents/agent_g_anomaly.py",
            "args": [
                "--bundle-dir", str(run_dir),
                "--run-dir", str(run_dir),
                "--out-dir", str(run_dir),
                "--history-dir", str(runs_root)
            ]
        },
        {
            "name": "Agent H (Triage)",
            "script": "agents/agent_h_exception_triage.py",
            "args": ["--bundle-dir", str(run_dir), "--run-dir", str(run_dir), "--out-dir", str(run_dir)]
        },
        {
            "name": "Agent I (Orchestrator)",
            "script": "agents/agent_i_orchestrator.py",
            "args": ["--bundle-dir", str(run_dir), "--run-dir", str(run_dir), "--out-dir", str(run_dir)]
        }
    ]

    # 3. Execute Pipeline
    print("-" * 50)
    for step in pipeline_steps:
        print(f"[{step['name']}] Starting...")
        cmd = [sys.executable, step['script']] + step['args']

        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            print(f"\n Pipeline Stopped: {step['name']} failed with exit code {e.returncode}")
            print(e.stderr)
            raise RuntimeError(f"{step['name']} failed: {e.stderr}")
        except FileNotFoundError:
            raise RuntimeError(f"Could not find script {step['script']}")

    print("-" * 50)
    print(f"Pipeline Complete. Run ID: {run_id}")

    # 4. Read Final Decision
    payload_path = run_dir / "posting_payload.json"
    if payload_path.exists():
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        action = payload.get("action", "UNKNOWN")
        print(f"FINAL DECISION: {action}")
    else:
        payload = {}
        action = "UNKNOWN"

    return {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "action": action,
        "payload": payload,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IIPS Pipeline Runner")
    parser.add_argument("bundle_dir", help="Path to input bundle (e.g. input_bundles/s01)")

    args = parser.parse_args()

    result = run_pipeline(args.bundle_dir)
    print(f"\nRun ID: {result['run_id']}")
    print(f"Action: {result['action']}")
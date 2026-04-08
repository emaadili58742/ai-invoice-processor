import json
import shutil
import tempfile
import os
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Ensure project root is importable
import sys
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from run import run_pipeline

app = FastAPI(title="IIPS - Intelligent Invoice Processing System", version="1.0.0")

# Allow Make.com and any frontend to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health_check():
    openai_configured = bool(os.environ.get("OPENAI_API_KEY"))
    return {
        "status": "healthy",
        "version": "1.0.0",
        "openai_configured": openai_configured,
    }


@app.post("/api/invoices/process")
async def process_invoice(
    file: UploadFile = File(...),
    po_data: str = Form(default=None),
    grn_data: str = Form(default=None),
):
    """Accept a PDF/image invoice, run the full pipeline, return the decision."""

    # Validate file type
    allowed_types = {".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif"}
    suffix = Path(file.filename).suffix.lower()
    if suffix not in allowed_types:
        raise HTTPException(400, f"Unsupported file type: {suffix}. Allowed: {allowed_types}")

    # Create a temporary input bundle directory
    tmp_bundle = Path(tempfile.mkdtemp(prefix="iips_upload_"))
    try:
        # Save uploaded invoice
        invoice_path = tmp_bundle / f"invoice{suffix}"
        content = await file.read()
        if len(content) == 0:
            raise HTTPException(400, "Uploaded file is empty")
        invoice_path.write_bytes(content)

        # Save optional PO data
        if po_data:
            (tmp_bundle / "purchase_order.json").write_text(po_data, encoding="utf-8")

        # Save optional GRN data
        if grn_data:
            (tmp_bundle / "grn.json").write_text(grn_data, encoding="utf-8")

        # Create a minimal manifest.yaml so Agent B can find the invoice
        manifest = (
            f"scenario_id: api_upload\n"
            f"description: Invoice uploaded via API\n"
            f"invoice_file: {invoice_path.name}\n"
            f"use_mock_extraction: false\n"
        )
        (tmp_bundle / "manifest.yaml").write_text(manifest, encoding="utf-8")

        # Copy shared config files into the bundle
        shared_dir = PROJECT_ROOT / "input_bundles" / "shared"
        if shared_dir.exists():
            for item in shared_dir.iterdir():
                if item.is_file():
                    shutil.copy2(item, tmp_bundle / item.name)

        # Run the pipeline
        result = run_pipeline(str(tmp_bundle))

        # Read extra details from the posting payload
        payload = result.get("payload", {})

        return {
            "run_id": result["run_id"],
            "status": "completed",
            "action": result["action"],
            "assigned_to": payload.get("assigned_to", ""),
            "total_findings": len(payload.get("findings_summary", [])),
            "invoice_id": payload.get("invoice_id", ""),
            "vendor_name": payload.get("vendor_name", ""),
            "total_amount": payload.get("total_amount", 0),
            "currency": payload.get("currency", ""),
        }

    except HTTPException:
        raise
    except RuntimeError as e:
        raise HTTPException(500, f"Pipeline error: {str(e)}")
    except Exception as e:
        raise HTTPException(500, f"Unexpected error: {str(e)}")
    finally:
        # Clean up temp bundle
        shutil.rmtree(tmp_bundle, ignore_errors=True)


@app.get("/api/invoices/{run_id}/status")
def get_invoice_status(run_id: str):
    """Get the status/decision for a completed run."""
    runs_dir = PROJECT_ROOT / "runs"
    # Find the run directory (run_id is the directory name)
    run_dir = runs_dir / run_id
    if not run_dir.exists():
        # Try partial match (run_id might not include timestamp)
        matches = list(runs_dir.glob(f"{run_id}*"))
        if not matches:
            raise HTTPException(404, f"Run not found: {run_id}")
        run_dir = matches[-1]  # Latest match

    payload_path = run_dir / "posting_payload.json"
    if not payload_path.exists():
        return {"run_id": run_id, "status": "processing"}

    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    return {
        "run_id": run_id,
        "status": "completed",
        "action": payload.get("action", "UNKNOWN"),
        "assigned_to": payload.get("assigned_to", ""),
        "total_findings": len(payload.get("findings_summary", [])),
        "invoice_id": payload.get("invoice_id", ""),
        "vendor_name": payload.get("vendor_name", ""),
        "total_amount": payload.get("total_amount", 0),
        "currency": payload.get("currency", ""),
    }


@app.get("/api/invoices/{run_id}/audit")
def get_invoice_audit(run_id: str):
    """Get full audit data for a completed run."""
    runs_dir = PROJECT_ROOT / "runs"
    run_dir = runs_dir / run_id
    if not run_dir.exists():
        matches = list(runs_dir.glob(f"{run_id}*"))
        if not matches:
            raise HTTPException(404, f"Run not found: {run_id}")
        run_dir = matches[-1]

    def read_if_exists(filename):
        p = run_dir / filename
        return p.read_text(encoding="utf-8") if p.exists() else None

    def read_json_if_exists(filename):
        p = run_dir / filename
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
        return None

    return {
        "audit_log_md": read_if_exists("audit_log.md"),
        "exceptions_md": read_if_exists("exceptions.md"),
        "metrics": read_json_if_exists("metrics.json"),
        "posting_payload": read_json_if_exists("posting_payload.json"),
    }

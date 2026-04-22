import { useState, useRef, DragEvent } from "react";
import { uploadLog } from "../api/pipeline";

interface FileUploadProps {
  onUploaded: () => void;
}

const FORMATS = ["JSON", "CSV", "XML", "LOG"];

export function FileUpload({ onUploaded }: FileUploadProps) {
  const [dragging, setDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleFile = async (file: File) => {
    setError(null);
    setSuccess(null);
    setUploading(true);
    try {
      const ext = file.name.split(".").pop()?.toLowerCase();
      const result = await uploadLog(file, ext);
      setSuccess(`Uploaded: ${result.file_name} · job ${result.job_id.slice(0, 8)}…`);
      onUploaded();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Upload failed");
    } finally {
      setUploading(false);
    }
  };

  const onDrop = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragging(false);
    const file = e.dataTransfer.files[0];
    if (file) handleFile(file);
  };

  return (
    <div>
      <div className="section-label">INGEST</div>
      <div
        className={`upload-zone${dragging ? " dragging" : ""}`}
        onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onDrop={onDrop}
      >
        <p className="upload-hint">Drop a log file here to upload</p>
        <p className="upload-sub">Supported formats</p>
        <div className="format-badges">
          {FORMATS.map((f) => (
            <span key={f} className="format-badge">{f}</span>
          ))}
        </div>
        <input
          ref={inputRef}
          type="file"
          accept=".json,.csv,.xml,.log,.txt"
          style={{ display: "none" }}
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (file) handleFile(file);
            e.target.value = "";
          }}
        />
        <button
          className="btn-upload"
          onClick={() => inputRef.current?.click()}
          disabled={uploading}
        >
          {uploading ? "Uploading…" : "Choose File"}
        </button>
        {error && <p className="upload-error">{error}</p>}
        {success && <p className="upload-success">{success}</p>}
      </div>
    </div>
  );
}

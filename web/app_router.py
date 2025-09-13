from pathlib import Path

def read(who: str, file_type: str = "html", filename = None):
    base = Path(f"web/{who}")
    if file_type == "css":
        return (base / "styles.css" if filename is None else (base / filename)).read_text(encoding="utf-8")
    if file_type == "js":
        return (base / "scripts.js" if filename is None else (base / filename)).read_text(encoding="utf-8")
    return (base / f"{who}.html").read_text(encoding="utf-8")
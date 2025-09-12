from pathlib import Path

def read(who: str, file_type: str = "html"):
    if file_type == "css":
        css_file = Path(f"web/app_{who}/styles.css").read_text(encoding="utf-8")
        return css_file
    html_file = Path(f"web/app_{who}/{who}.html").read_text(encoding="utf-8")
    return html_file
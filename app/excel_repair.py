import re
import zipfile
import tempfile
from pathlib import Path

BAD_ACTIVE_PANES = {
    "bottom-right": "bottomRight",
    "top-right": "topRight",
    "top-left": "topLeft",
    "bottom-left": "bottomLeft",
}

def repair_xlsx_bytes(src_bytes: bytes) -> bytes:
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)

        src_path = tmpdir / "src.xlsx"
        src_path.write_bytes(src_bytes)

        unpack_dir = tmpdir / "unpacked"
        unpack_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(src_path, "r") as z:
            z.extractall(unpack_dir)

        # 1) worksheets: activePane
        sheets_dir = unpack_dir / "xl" / "worksheets"
        if sheets_dir.exists():
            for sheet in sheets_dir.glob("sheet*.xml"):
                text = sheet.read_text(encoding="utf-8")
                for bad, good in BAD_ACTIVE_PANES.items():
                    text = text.replace(f'activePane="{bad}"', f'activePane="{good}"')
                sheet.write_text(text, encoding="utf-8")

        # 2) styles: remove ALL alignment tags (both <alignment .../> and <alignment>...</alignment>)
        styles_path = unpack_dir / "xl" / "styles.xml"
        if styles_path.exists():
            text = styles_path.read_text(encoding="utf-8")
            text = re.sub(r"<alignment\b[^>]*/>", "", text)
            text = re.sub(r"<alignment\b[^>]*>.*?</alignment>", "", text, flags=re.DOTALL)
            styles_path.write_text(text, encoding="utf-8")

        fixed_path = tmpdir / "fixed.xlsx"
        with zipfile.ZipFile(fixed_path, "w", zipfile.ZIP_DEFLATED) as zout:
            for file in unpack_dir.rglob("*"):
                zout.write(file, file.relative_to(unpack_dir))

        return fixed_path.read_bytes()

from pathlib import Path

try:
    from PIL import Image
except Exception as exc:  # pragma: no cover
    raise SystemExit("Pillow no esta instalado") from exc


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    png_path = root / "logo" / "logo_reca.png"
    ico_path = root / "logo" / "logo_reca.ico"
    if not png_path.exists():
        raise SystemExit(f"No se encontro {png_path}")
    image = Image.open(png_path)
    image.save(ico_path, format="ICO", sizes=[(256, 256)])
    print(f"Ico generado en {ico_path}")


if __name__ == "__main__":
    main()

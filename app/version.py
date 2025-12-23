from app.paths import resource_path


def get_version() -> str:
    try:
        path = resource_path("VERSION")
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        return "0.0.0"

"""Auto-import all step modules so their @step decorators register."""
import importlib
import re
from pathlib import Path


def _step_sort_key(path):
    """Extract numeric prefix for natural sorting (v1 < v2 < ... < v10)."""
    m = re.match(r'v(\d+)', path.stem)
    return int(m.group(1)) if m else 0


for _f in sorted(Path(__file__).resolve().parent.glob("v*.py"), key=_step_sort_key):
    importlib.import_module(f".{_f.stem}", package=__name__)

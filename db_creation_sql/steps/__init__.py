"""Auto-import all step modules so their @step decorators register."""
import importlib
from pathlib import Path

for _f in sorted(Path(__file__).resolve().parent.glob("step_*.py")):
    importlib.import_module(f".{_f.stem}", package=__name__)

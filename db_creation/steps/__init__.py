"""Auto-import all step modules so their @step decorators register."""
from pathlib import Path
import importlib

for _f in sorted(Path(__file__).resolve().parent.glob("v*.py")):
    importlib.import_module(f".{_f.stem}", package=__name__)

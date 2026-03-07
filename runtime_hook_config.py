# PyInstaller runtime hook — runs before drawing_viewer.py starts.
#
# Maps the `config` module name to `_runtime_config` so that all existing
# `import config` / `from config import ...` statements in the app work
# unchanged when running as a frozen executable.
import sys
import _runtime_config

sys.modules["config"] = _runtime_config

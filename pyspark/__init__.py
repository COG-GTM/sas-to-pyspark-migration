"""Namespace shim for the repo-local ``pyspark`` directory.

This directory shares its top-level name with the pip-installed ``pyspark``
distribution. When scripts and tests are run from the repository root, Python
resolves ``import pyspark`` to *this* directory, which would otherwise hide the
real Spark API.

To let both worlds coexist we:

1. Use :func:`pkgutil.extend_path` to add the installed distribution's directory
   to this package's ``__path__`` (so ``pyspark.sql``, ``pyspark.ml`` and the
   repo-local ``pyspark.common`` all resolve), and
2. Execute the installed package's ``__init__`` in this module's namespace so
   top-level names such as ``SparkContext`` and ``__version__`` remain available.
"""

import os as _os
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

_this_dir = _os.path.dirname(_os.path.abspath(__file__))
for _entry in __path__:
    if _os.path.abspath(_entry) == _this_dir:
        continue
    _installed_init = _os.path.join(_entry, "__init__.py")
    if _os.path.isfile(_installed_init):
        with open(_installed_init, encoding="utf-8") as _fh:
            exec(compile(_fh.read(), _installed_init, "exec"))
        break

del _os, _entry, _this_dir, _installed_init, _fh

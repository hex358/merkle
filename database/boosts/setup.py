from setuptools import setup
from pybind11.setup_helpers import Pybind11Extension, build_ext
from pybind11 import get_include


ext_modules = [
    Pybind11Extension(
        name="db_boosts",
        sources=["db_boosts.cpp"],
            cxx_std=20,
        include_dirs=[get_include(), r"D:merkle\merkleproof\.venv\py\main\nyada-db\boosts\bitsery\include"],
        #language="c++"
        extra_compile_args=[
            "/O2",
        ],
    ),
]

setup(
    name="db_boosts",
    version="1.0.0",
    author="hex358",
    description="c++ speedups for python database",
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
)
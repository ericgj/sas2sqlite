import codecs
import os.path
from setuptools import setup, find_packages

# Note: copied from pip setup.py


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


install_requires = ["sas7bdat", "julian"]

tests_require = ["pytest"]


setup(
    name="sas2sqlite3",
    version=get_version("src/sas2sqlite3/__init__.py"),
    author="Eric Gjertsen",
    author_email="ericgj72@gmail.com",
    description="Import sas7bdat files to sqlite3 dbase",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    url="https://github.com/ericgj/sas2sqlite.git",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Development Status :: 2 - Pre-Alpha",
        "License :: OSI Approved :: MIT License",
    ],
    entry_points={
        "console_scripts": [
            "sas2sqlite=sas2sqlite3.__main__:main",
        ],
    },
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require={"test": tests_require},  # for pip
    zip_safe=False,  # for mypy
)

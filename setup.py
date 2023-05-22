#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="tap-nikabot",
    version="1.0.5",
    description="Singer.io tap for extracting data from Nikabot",
    python_requires=">=3.6.0",
    author="Paul Heasley",
    author_email="paul@phdesign.com.au",
    url="https://github.com/singer-io/tap-nikabot",
    keywords=["nikabot", "singer", "stitch", "tap"],
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_nikabot"],
    install_requires=["singer-python==5.9.0", "requests==2.31.0", "backoff==1.8.0"],
    extras_require={
        "dev": [
            "pylint",
            "ipdb",
            "black~=19.10b0",
            "coverage~=5.1",
            "flake8~=3.8.3",
            "mypy~=0.780",
            "pytest~=5.4.3",
            "pytest-socket~=0.3.5",
            "requests-mock~=1.8.0",
        ]
    },
    entry_points="""
    [console_scripts]
    tap-nikabot=tap_nikabot:main
    """,
    packages=find_packages(exclude=("tests",)),
    package_data={"schemas": ["tap_nikabot/schemas/*.json"]},
    include_package_data=True,
)

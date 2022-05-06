#!/usr/bin/env python
from setuptools import setup

setup(
    name="singer-target-postgresql",
    version="0.1.0",
    description="Singer.io target for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["singer_target_postgresql"],
    install_requires=[
        "singer-python>=5.0.12",
    ],
    entry_points="""
    [console_scripts]
    singer-target-postgresql=singer_target_postgresql:main
    """,
    packages=["singer_target_postgresql"],
    package_data = {},
    include_package_data=True,
)

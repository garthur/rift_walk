from setuptools import setup

setup(name = "rift_walk",
      packages = ["src"],
      version = "0.0.1.dev",
      entry_points={
            "console_scripts": ["rift-walk-cli=src.utils.cli:main"]
      }
)
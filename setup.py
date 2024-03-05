from setuptools import find_packages, setup

setup(
    name="lemmyw04b6eb792ca4a1",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "exorde_data",
        "aiohttp",
        "wordsegment==1.3.1"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
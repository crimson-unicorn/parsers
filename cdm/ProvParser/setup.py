import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="provparser",
    version="0.0.1",
    author="Xueyuan Michael Han",
    author_email="hanx@g.harvard.edu",
    description="Parse audit provenance data from CDM and CamFlow.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/crimson-unicorn/parsers/tree/master/cdm",
    install_requires=[
        'ijson',
        'cffi',
        'xxhash',
        'yappi',
        'python-rocksdb',
        'tqdm',
    ],
    dependency_links=[
          "https://github.com/isagalaev/ijson/tarball/e252a50#egg=ijson-2.4",
    ],
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Natural Language :: English",
        "Programming Language :: Python :: 2.7",
        "Operating System :: OS Independent",
    ],
)
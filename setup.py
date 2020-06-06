import setuptools

setuptools.setup(
    name="cic_util",
    version="0.0.4",
    author="CICData",
    author_email="data@cicdata.vn",
    description="An util package",
    url="https://github.com/cicdata-io/etl",
    packages=setuptools.find_packages(),
    install_requires=[
          'pyspark', 'py4j'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
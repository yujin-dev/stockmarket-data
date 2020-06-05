import setuptools

with open("README.md", "r", encoding='UTF8') as fh:
    long_description = fh.read()

setuptools.setup(
    name="stock data sourcing and checking", # Replace with your own username
    version="0.0.1",
    author="yujinlee",
    description="sourcnig for stock data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://https://github.com/yujin-dev/stockmarket-data",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: Window",
    ],
    python_requires='>=3.6',
)
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="schedulusv2",
    version="0.0.1",
    author="",
    author_email="",
    description="",
    long_description="long_description",
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(),
    install_requires=[
        'pandas',
        'simulus',
        'pandas',
        'tqdm',
        'matplotlib',
        'pillow'  # Add Pillow for Tkinter
    ],
    entry_points='''
        [console_scripts]
        schedulus=src.main:cli
    ''',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
from setuptools import setup, find_packages

with open('requirements.txt') as f:
    required = [line.strip() for line in f.readlines()]

setup(
    name="analithops",
    version="0.1.0",
    author="Kamilbur",
    description="Package to analyze lithops stats dumped to jsonl format",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Workrooo/analithops",
    packages=find_packages(),
    install_requires=required,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)


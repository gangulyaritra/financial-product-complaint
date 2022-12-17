from setuptools import find_packages, setup
from typing import List

# Declare variables for setup functions.
PROJECT_NAME = "financial-product-complaint"
VERSION = "0.0.1"
AUTHOR = "Aritra Ganguly"
AUTHOR_EMAIL = "aritraganguly.msc@protonmail.com"
DESCRIPTION = "Financial Product Complaint"


def get_requirements_list() -> List[str]:
    """
    This function returns a list of strings as requirements from the requirements.txt file.
    """
    with open("requirements.txt") as f:
        requirement_list = [line.replace("\n", "") for line in f.readlines()]
        if "-e ." in requirement_list:
            requirement_list.remove("-e .")
        return requirement_list


setup(
    name=PROJECT_NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    description=DESCRIPTION,
    packages=find_packages(),
    install_requires=get_requirements_list(),
)

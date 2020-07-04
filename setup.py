from setuptools import find_packages, setup

setup(
    author = 'Mustafa Eskin',
    author_email = 'mustafasamedeskin@gmail.com',
    name='JiraWrapper',
    version = '1.0',
    packages=find_packages(where="src"),
    package_dir={"": "src"},
)
from setuptools import setup, find_packages

REQUIRED_PKGS = ['requests',
                 'pandas',
                 'pyarrow',
                 'apache-airflow',
                 'streamlit'
                 ]

setup(
    name='weather_application',
    version='0.0.1',
    long_description=open("README.md", "r", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    url='https://www.thiagosilva.tech',
    license='',
    author='thiago.silva',
    author_email='',
    description='',
    install_requires=REQUIRED_PKGS,
    python_requires=">=3.11.0",
    include_package_data=True,
    zip_safe=False
)

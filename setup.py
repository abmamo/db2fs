from setuptools import setup, find_packages

# read the contents of README file
from os import path
# get current file directory
this_directory = path.abspath(path.dirname(__file__))
# open README with UTF-8 encoding
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    # read README
    long_description = f.read()

setup(
      name='db2fs',
      version='0.0.1',
      description='db to file extraction',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='http://github.com/abmamo/fs2db',
      author='Abenezer Mamo',
      author_email='contact@abmamo.com',
      license='MIT',
      packages=find_packages(exclude=("tests",)),
      include_package_data=True,
      package_data={
          "db2fs": [
            "logging.cfg"
          ]
      },
      install_requires=[
          # data manipulation
          "pandas==1.1.4",
          "tqdm==4.55.0",
          # db orm
          "sqlalchemy==1.3.20",
          "pyarrow==2.0.0",
          "xlrd==2.0.1",
          # db engines
          "psycopg2-binary==2.8.6",
          "pymysql==0.10.1",
          # env
          "python-dotenv==0.15.0",
          # cloub connectors
          "azure-storage-blob==12.5.0",
          "google-cloud-storage==1.35.0",
          "boto3==1.16.56",
          # testing
          "pytest==6.2.1",
          "pytest-cov==2.10.1",
          "mock @ https://github.com/abmamo/mock/archive/v0.0.1.tar.gz",
          "fastdb @ https://github.com/abmamo/fastdb/archive/v0.0.1.tar.gz"
          ],
      zip_safe=False
)

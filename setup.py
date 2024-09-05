import pathlib
import setuptools

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setuptools.setup(
      name='edu_edfi_airflow',
      version='0.4.0',

      description='EDU Airflow tools for Ed-Fi',
      license_files=['LICENSE.md'],
      url='https://github.com/edanalytics/edu_edfi_airflow',

      author='Jay Kaiser, Erik Joranlien',
      author_email='jkaiser@edanalytics.org, ejoranlien@edanalytics.org',

      long_description=README,
      long_description_content_type='text/markdown',
      keyword='edu, edfi, ed-fi, airflow, s3, snowflake, data',

      packages=setuptools.find_namespace_packages(include=['edu_edfi_airflow', 'edu_edfi_airflow.*']),
      install_requires=[
          'ea_airflow_util',
          'edfi_api_client',
          'croniter',
      ],
      zip_safe=False,
)

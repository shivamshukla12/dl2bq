from setuptools import setup
# calling the setup function 
setup(name='dl2bq',
      version='0.0.1',
      description='A Data Migration API for Data Lake to BQ',
      py_modules=["dl2bq"],
      author='shivam shukla',
      author_email='shivamshukla12@gmail.com',
      package_dir = {' ':r'dl2bq'}
      )

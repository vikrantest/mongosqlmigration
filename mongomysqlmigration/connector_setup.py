from setuptools import setup, find_packages, Extension
from Cython.Distutils import build_ext


ext_modules=[
    Extension('db_connectors',['db_connectors.pyx']) ]

setup(name='db connector package',
      packages=find_packages(),
      cmdclass = {'build_ext': build_ext},
      ext_modules = ext_modules,
     )

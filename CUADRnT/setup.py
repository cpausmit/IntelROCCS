#!/usr/bin/env python

"""
Standard python setup.py file for CUADRnT package
To build     : python setup.py build
To install   : python setup.py install --prefix=<some dir>
To clean     : python setup.py clean
To build doc : python setup.py doc
To run tests : python setup.py test
"""

# FIXME: (10) General cleanup of script

# system modules
import os
import re
import sys
import shutil
import subprocess
from os.path import join as pjoin
from unittest import TextTestRunner, TestLoader
from distutils.core import setup
from distutils.cmd import Command
from distutils.command.install import INSTALL_SCHEMES

# package modules

version = '1.0.0'  # TODO: (10) Set up automatic versioning system
required_python_version = '2.7'

class TestCommand(Command):
    """
    Class to handle unit tests
    """
    user_options = []

    def initialize_options(self):
        """Init method"""
        dir_ = os.path.dirname(os.path.realpath(__file__))
        self.test_dir = dir_ + '/test'

    def finalize_options(self):
        """Finalize method"""
        pass

    def run(self):
        """
        Finds all the tests modules in test/, and runs them.
        """
        tests = TestLoader().discover(start_dir=self.test_dir, pattern='*_t.py')
        TextTestRunner(verbosity=2).run(tests)

        # remove test pyc files
        pyc_re = re.compile('^.*.pyc$')
        for file_ in os.listdir(self.test_dir):
            if pyc_re.match(file_):
                os.remove('%s/%s' % (self.test_dir, file_))

class CleanCommand(Command):
    """
    Class which clean-up all pyc files
    """
    user_options = []

    def initialize_options(self):
        """Init method"""
        self._clean_me = []
        for root, dirs, files in os.walk('.'):
            for fname in files:
                if fname.endswith('.pyc') or fname. endswith('.py~') or fname.endswith('.rst~'):
                    self._clean_me.append(pjoin(root, fname))
            for dname in dirs:
                if dname == 'build':
                    self._clean_me.append(pjoin(root, dname))

    def finalize_options(self):
        """Finalize method"""
        pass

    def run(self):
        """Run method"""
        for clean_me in self._clean_me:
            try:
                if os.path.isdir(clean_me):
                    shutil.rmtree(clean_me)
                else:
                    os.unlink(clean_me)
            except:
                pass
class DocCommand(Command):
    """
    Class which build documentation
    """
    user_options = []

    def initialize_options(self):
        """Init method"""
        pass

    def finalize_options(self):
        """Finalize method"""
        pass

    def run(self):
        """Run method"""
        cdir = os.getcwd()
        os.chdir(os.path.join(cdir, 'doc'))
        os.environ['PYTHONPATH'] = os.path.join(cdir, 'src/python')
        subprocess.call('make html', shell=True)
        os.chdir(cdir)

def dirwalk(relative_dir):
    """
    Walk a directory tree and look-up for __init__.py files.
    If found yield those dirs. Code based on
    http://code.activestate.com/recipes/105873-walk-a-directory-tree-using-a-generator/
    """
    idir = os.path.join(os.getcwd(), relative_dir)
    for fname in os.listdir(idir):
        full_path = os.path.join(idir, fname)
        if os.path.isdir(full_path) and not os.path.islink(full_path):
            for subdir in dirwalk(full_path):  # recurse into subdir
                yield subdir
        else:
            initdir, initfile = os.path.split(full_path)
            if initfile == '__init__.py':
                yield initdir

def find_packages(relative_dir):
    "Find list of packages in a given dir"
    packages = []
    for idir in dirwalk(relative_dir):
        package = idir.replace(os.getcwd() + '/', '')
        package = package.replace(relative_dir + '/', '')
        package = package.replace('/', '.')
        packages.append(package)
    return packages

def datafiles(idir):
    """Return list of data files in provided relative dir"""
    files = []
    for dirname, dirnames, filenames in os.walk(idir):
        if dirname.find('.svn') != -1:
            continue
        for subdirname in dirnames:
            if subdirname.find('.svn') != -1:
                continue
            files.append(os.path.join(dirname, subdirname))
        for filename in filenames:
            if filename[-1] == '~':
                continue
            files.append(os.path.join(dirname, filename))
    return files

def main():
    """
    Main function
    """
    name = 'CUADRnT'

    log_path = '%s/log' % (os.environ['CUADRNT_ROOT'])
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    if sys.version < required_python_version:
        msg = "I'm sorry, but %s %s requires Python %s or later."
        print msg % (name, version, required_python_version)
        sys.exit(1)

    description = "CUADRnT is CMS Usage Analytics and Data Replication Tools"
    url = "https://github.com/cpausmit/IntelROCCS/blob/v2/CUADRnT/"
    readme = "https://github.com/cpausmit/IntelROCCS/blob/v2/CUADRnT/README.md"
    author = "Bjorn Barrefors",
    author_email = "bjorn [dot] peter [dot] barrefors [AT] cern [dot] ch",
    keywords = ["CUADRnT"]
    package_dir = {'UADR': 'src/python/UADR'}
    packages = find_packages('src/python')
    data_files = []  # list of tuples whose entries are (dir, [data_files])
    cms_license = "CMS experiment software"
    classifiers = [
        "Development Status :: 3 - Production/Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: CMS/CERN Software License",
        "Operating System :: MacOS :: MacOS X",
        #"Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Programming Language :: Python"
    ]

    # set default location for "data_files" to
    # platform specific "site-packages" location
    for scheme in INSTALL_SCHEMES.values():
        scheme['data'] = scheme['purelib']

    setup(
        name=name,
        version=version,
        description=description,
        long_description=readme,
        keywords=keywords,
        packages=packages,
        package_dir=package_dir,
        data_files=data_files,
        scripts=datafiles('bin'),
        requires=['python (>=2.7)'],
        classifiers=classifiers,
        cmdclass={'test':TestCommand, 'clean':CleanCommand, 'doc':DocCommand},
        author=author,
        author_email=author_email,
        url=url,
        license=cms_license,
    )

if __name__ == "__main__":
    main()

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
import logging
import logging.handlers
import os
import re
import sys
import pwd
import grp
import shutil
import ConfigParser
from subprocess import call
from os.path import join as pjoin
from unittest import TextTestRunner, TestLoader
from distutils.core import setup
from distutils.cmd import Command
from distutils.dir_util import mkpath

version = '1.0'  # TODO: (10) Set up automatic versioning system
required_python_version = '2.7'

class TestCommand(Command):
    """
    Class to handle unit tests
    """
    user_options = []

    def initialize_options(self):
        """Init method"""
        logging.basicConfig(filename='/var/log/CUADRnT/cuadrnt-test.log', filemode='w', format='%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M', level=logging.DEBUG)
        self.test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test')

    def finalize_options(self):
        """Finalize method"""
        self.tests = TestLoader().discover(start_dir=self.test_dir, pattern='*_t.py')

    def run(self):
        """
        Finds all the tests modules in test/, and runs them.
        """
        TextTestRunner(verbosity=2).run(self.tests)
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
        call('make html', shell=True)
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
    for directory in dirwalk(relative_dir):
        package = directory.replace(os.getcwd() + '/', '')
        package = package.replace(relative_dir + '/', '')
        package = package.replace('/', '.')
        packages.append(package)
    return packages

def find_files(relative_dir):
    """Return list of data files in provided relative dir"""
    files = []
    for dir_name, subdir_names, file_names in os.walk(relative_dir):
        for file_name in file_names:
            files.append(os.path.join(dir_name, file_name))
    return files

def main(argv):
    """
    Main setup function
    """
    name = 'CUADRnT'

    if not sys.version[:3] == required_python_version:
        print "I'm sorry, but %s %s requires Python %s." % (name, version, required_python_version)
        sys.exit(1)

    # get setup config file
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'etc/setup.cfg')
    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(config_file)
    username = config_parser.get('permissions', 'username')
    group = config_parser.get('permissions', 'group')

    description = "CUADRnT is CMS Usage Analytics and Data Replication Tools"
    url = "https://github.com/cpausmit/IntelROCCS/blob/v2/CUADRnT/"
    readme = "https://github.com/cpausmit/IntelROCCS/blob/v2/CUADRnT/README.md"
    author = "Bjorn Barrefors",
    author_email = "bjorn [dot] peter [dot] barrefors [AT] cern [dot] ch",
    keywords = ["CUADRnT"]
    package_dir = {'': 'src/python'}
    packages = find_packages('src/python')
    data_files = [('/usr/local/bin', find_files('bin')),
                  ('/var/opt/CUADRnT', find_files('etc'))]
    scripts = []
    cms_license = "CMS experiment software"
    classifiers = [
        "Development Status :: 3 - Production/Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: CMS/CERN Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python"
    ]

    setup(
        name=name,
        version=version,
        description=description,
        long_description=readme,
        keywords=keywords,
        packages=packages,
        package_dir=package_dir,
        data_files=data_files,
        scripts=scripts,
        requires=['python (>=2.7)'],
        classifiers=classifiers,
        cmdclass={'test':TestCommand, 'clean':CleanCommand, 'doc':DocCommand},
        author=author,
        author_email=author_email,
        url=url,
        license=cms_license,
    )

    mkpath('/var/lib/CUADRnT')
    mkpath('/var/log/CUADRnT')
    uid = pwd.getpwnam(username).pw_uid
    gid = grp.getgrnam(group).gr_gid
    os.chown('/var/lib/CUADRnT', uid, gid)
    os.chown('/var/log/CUADRnT', uid, gid)

if __name__ == "__main__":
    main(sys.argv[1:])

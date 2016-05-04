#!/usr/bin/env python
"""
Standard python setup.py file for cuadrnt package
To build     : python setup.py build
To install   : python setup.py install --prefix=<some dir>
To clean     : python setup.py clean
To build doc : python setup.py doc
To run tests : python setup.py test
"""

# system modules
#import logging
import os
#import re
import sys
import pwd
import grp
import shutil
import ConfigParser
from subprocess import call
from os.path import join as pjoin
# from unittest import TextTestRunner, TestLoader
from setuptools import setup
from distutils.cmd import Command
from distutils.dir_util import mkpath
#from logging.handlers import TimedRotatingFileHandler

version = '2.0'
#required_python_version = '2.7'

# class TestCommand(Command):
#     """
#     Class to handle unit tests
#     """
#     user_options = []

#     def initialize_options(self):
#         """Init method"""
#         log_path = '/var/log/cuadrnt'
#         log_file = 'cuadrnt-test.log'
#         file_name = '%s/%s' % (log_path, log_file)
#         self.logger = logging.getLogger()
#         self.logger.setLevel(logging.DEBUG)
#         handler = TimedRotatingFileHandler(file_name, when='h', interval=1, backupCount=2)
#         formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
#         handler.setFormatter(formatter)
#         self.logger.addHandler(handler)
#         self.test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test')

#     def finalize_options(self):
#         """Finalize method"""
#         self.tests = TestLoader().discover(start_dir=self.test_dir, pattern='*_t.py')

#     def run(self):
#         """
#         Finds all the tests modules in test/, and runs them.
#         """
#         TextTestRunner(verbosity=2).run(self.tests)
#         # remove test pyc files
#         pyc_re = re.compile('^.*.pyc$')
#         for file_ in os.listdir(self.test_dir):
#             if pyc_re.match(file_):
#                 os.remove('%s/%s' % (self.test_dir, file_))

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

def find_modules(relative_dir):
    """
    Walk a directory tree and look-up for __init__.py files.
    If found yield those dirs. Code based on
    http://code.activestate.com/recipes/105873-walk-a-directory-tree-using-a-generator/
    """
    idir = os.path.join(os.getcwd(), relative_dir)
    for fname in os.listdir(idir):
        full_path = os.path.join(idir, fname)
        if os.path.isdir(full_path) and not os.path.islink(full_path):
            for subdir in find_modules(full_path):  # recurse into subdir
                yield subdir
        else:
            initdir, initfile = os.path.split(full_path)
            if initfile == '__init__.py':
                yield initdir

def find_packages(relative_dir):
    "Find list of packages in a given dir"
    packages = []
    for directory in find_modules(relative_dir):
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
    name = 'cuadrnt'

    # if not sys.version[:3] == required_python_version:
    #     print "I'm sorry, but %s %s requires Python %s." % (name, version, required_python_version)
    #     sys.exit(1)

    # get setup config file
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'etc/setup.cfg')
    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(config_file)
    username = config_parser.get('permissions', 'username')
    group = config_parser.get('permissions', 'group')

    description = "cuadrnt is CMS Usage Analytics and Data Replication Tools"
    url = "https://github.com/cpausmit/IntelROCCS/blob/v2/cuadrnt/"
    readme = "https://github.com/cpausmit/IntelROCCS/blob/v2/cuadrnt/README.md"
    author = "Bjorn Barrefors",
    author_email = "bjorn [dot] peter [dot] barrefors [AT] cern [dot] ch",
    keywords = ["cuadrnt"]
    package_dir = {'': 'src/python'}
    install_requires = [
        'setuptools',
        'pymongo',
        'MySQL-python',
        'sklearn',
    ]
    # numpy should also be installed but seems to be some bug in setuptools to install it
    packages = find_packages('src/python')
    data_files = [
        ('/usr/local/bin', find_files('bin')),
        ('/var/opt/cuadrnt', find_files('etc')),
        ('/var/lib/cuadrnt', find_files('data'))
    ]
    scripts = []
    cms_license = 'CMS experiment software'
    classifiers = [
        'Development Status :: 4 - Beta/Development',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: CMS/CERN Software License',
        'Environment :: Console',
        'Operating System :: MacOS :: MacOS X'
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2.7',
        'Topic :: System :: Distributed Computing'
    ]

    # Make sure folders exist
    mkpath('/var/lib/cuadrnt')
    mkpath('/var/log/cuadrnt')

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
        install_requires=install_requires,
        classifiers=classifiers,
        cmdclass={'clean':CleanCommand, 'doc':DocCommand},
        author=author,
        author_email=author_email,
        url=url,
        license=cms_license,
    )

    # Make sure the permissions are correct for folders
    uid = pwd.getpwnam(username).pw_uid
    gid = grp.getgrnam(group).gr_gid
    path = '/var/lib/cuadrnt'
    for root, dirs, files in os.walk(path):
        for momo in dirs:
            os.chown(os.path.join(root, momo), uid, gid)
        for momo in files:
            os.chown(os.path.join(root, momo), uid, gid)
    path = '/var/log/cuadrnt'
    for root, dirs, files in os.walk(path):
        for momo in dirs:
            os.chown(os.path.join(root, momo), uid, gid)
        for momo in files:
            os.chown(os.path.join(root, momo), uid, gid)

if __name__ == "__main__":
    main(sys.argv[1:])

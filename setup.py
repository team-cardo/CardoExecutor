from setuptools import find_packages, setup

version = "1.3.5"
pkgname = "CardoExecutor"
pkgs = find_packages()
pkgstatus = "Develop"

setup(name=pkgname,
	version=version,
	description="Cardo Executor Library",
	author='CardoTeam',
	author_email='',
	license='Apache License 2.0',
	packages=pkgs,
	platforms='Linux; Windows',
	classifiers=[],
	install_requires=[
		'cmreshandler',
		'networkx==2.2.0',
		'elasticsearch==5.4.0',
		'cardoutils3',
		'pydot==1.2.*',
	],
	dependency_links=[
		'your link to cmreslogging repository',
        'yout link to cardoutils3 repository',
		'your link to pypi server'
	]
)

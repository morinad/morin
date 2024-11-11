from setuptools import setup, find_packages

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='morin',
    version='0.2.27',
    packages=find_packages(),
    description='Помощь в подключениях и загрузке в БД',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Александр Морин',
    author_email='y.director@yandex.ru',
    url='https://github.com/morinad/morin',
    install_requires=['requests', 'clickhouse_connect', 'pandas', 'python-dateutil'],
)
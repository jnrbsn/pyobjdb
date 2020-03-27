from setuptools import setup


setup(
    name='pyobjdb',
    version='0.0.1',
    description='Simple Python key-value object database',
    long_description='Simple Python key-value object database',
    url='https://github.com/jnrbsn/pyobjdb',
    author='Jonathan Robson',
    author_email='jnrbsn@gmail.com',
    license='MIT',
    packages=['pyobjdb'],
    install_requires=[
        'msgpack',
        'plyvel',
    ],
    extras_require={
        'test': [
            'flake8',
            'freezegun',
            'pytest',
            'pytest-cov',
        ],
    },
)

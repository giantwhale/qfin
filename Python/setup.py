from distutils.core import setup

setup(name='qfin',
      version='0.01',
      description='Quantitative Finance Toolbox',
      author='Yue Zhao',
      author_email='yzhao0527@gmail.com',
      packages=[
            'qfin',
            'qfin.CryptoCrncy',
            'qfin.CryptoCrncy.exchanges',
            'qfin.CryptoCrncy.exchanges.gdax',
            'qfin.CryptoCrncy.strategy',
            'qfin.CryptoCrncy.data_loader',
            'qfin.utils', 
        ],
      package_data={'qfin.CryptoCrncy': ['templates/*.csv']},
     )

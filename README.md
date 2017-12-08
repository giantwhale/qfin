# Summary

This is a package I was developing in my spare time.  It is a Bitcoin trading robot that automates the following processes (all runs continuously):

1. Downloading data from Bitcoin exchanges 
2. Generating real time trade signals
3. Trade Bitcoin 
4. Record profit & loss and positions for performance analysis 

------------------------------------------------------

# qfin - Quantitative Finance Toolbox

## Python Package

To use this package, you should first create a `qfin/config.py`. You shall copy from the `qfin/config_template.py` file, but you would need to replace the fields with your own credential. You should at least change:

* GDAX public/private API keys.

Currently the DataEngine/GDAX should work. It pulls data from the GDAX exchange and aggregate to 5min bars.

```python
# in qfin/python
python examples/gdax/prod_start_gdax_data_server.py
```

# Bug Tracker

Sometimes one or more products has 'seconds to next update' less than 0.


import pip
import sys

parent = '/opt/spark/work-dir'
pip.main(['uninstall', 'pycrypto', 'Crypto'])
if "--public-key" in sys.argv:
    print("Using new launch_ipykernel-v3.2.2.py")
    pip.main(['install', '--target', '/opt/spark/work-dir', 'xyzservices', 'geopandas', 'bokeh', 'pycryptodomex', 'future', 'IPython', 'ipython_genutils', 'ipykernel', 'jedi', 'jupyter_client', 'jupyter_core', 'parso', 'pexpect', 'pickleshare', 'prompt-toolkit', 'ptyprocess', 'pygments', 'python-dateutil', 'pyzmq', 'setuptools', 'six', 'traitlets', 'wcwidth'])
    path = 'launch_ipykernel-v3.2.2.py'
else:
    print("Using launch_ipykernel-v2.3.1.py")
    pip.main(['install', '--target', '/opt/spark/work-dir', 'xyzservices', 'geopandas', 'bokeh', 'pycryptodome', 'future', 'IPython', 'ipython_genutils', 'ipykernel', 'jedi', 'jupyter_client', 'jupyter_core', 'parso', 'pexpect', 'pickleshare', 'prompt-toolkit', 'ptyprocess', 'pygments', 'python-dateutil', 'pyzmq', 'setuptools', 'six', 'traitlets', 'wcwidth'])
    path = 'launch_ipykernel-v2.3.1.py'

with open(f'{parent}/{path}', 'r') as f:
    script = f.read()

exec(script)
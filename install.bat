del /Q dist
del /Q iqfeed.egg-info
python setup.py bdist_wheel
pip uninstall iqfeed -y
pip install iqfeed --no-index --find-links dist
del /Q dist
del /Q iqfeed.egg-info

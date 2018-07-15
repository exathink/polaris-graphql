### The test folder is organized as follows

1. Put your test files in this directory. The top level pytest.ini file points to this directory as the
   the test source directory. 

2. In the hooks directory contain scripts that you can use to customize the test runner for this package. 
   
   1. test_setup.sh: If this file is present, it will be run before your test suite runs. 
   2. run_tests.sh:  If this file is present, it will replace the default test run operation 
      for the package: `package up package`. If you want to still delegate to this default test runner after doing
      any customizations specific to your package you can do so, and then call this default operation to run the default
      test suite. 
   3. test_teardown.sh: If this file is present it will be run exactly once after your test suite runs. Otherwise the
      default teardown operation `package down` will be run exactly once per test run. 

    The repository scaffolds contain files _test_setup.sh, _test_teardown.sh and _run_tests.sh as placeholders for these
    hooks. Rename these files and add your customizations. 
    
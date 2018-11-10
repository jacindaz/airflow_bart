import os
import pytest

AIRFLOW_DIRECTORY = os.path.dirname(os.path.realpath('__file__'))


def run_tests():
    tests_file_path = AIRFLOW_DIRECTORY + "/tests"
    print(f"Test directory: {tests_file_path}")
    os.system(f"pytest {tests_file_path}")

run_tests()

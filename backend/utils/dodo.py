def task_compile_prod_requirements():
    """generate prod requirements for service"""
    return {
        "file_dep": ["../requirements.prod.in"],
        "actions": ["../venv/bin/pip-compile -o ../requirements.txt ../requirements.prod.in"],
    }


def task_compile_test_requirements():
    """generate test requirements for service"""
    return {
        "file_dep": ["../requirements.prod.in", "../requirements.test.in"],
        "actions": ["../venv/bin/pip-compile -o ../requirements.test.txt ../requirements.test.in"],
    }


def task_compile_dev_requirements():
    """generate dev requirements for service"""
    return {
        "file_dep": ["../requirements.prod.in", "../requirements.dev.in", "../requirements.test.in"],
        "actions": ["../venv/bin/pip-compile -o ../requirements.dev.txt ../requirements.dev.in"],
    }


def task_sync_dev_requirements():
    """generate dev requirements for service"""
    return {
        "file_dep": ["../requirements.dev.txt"],
        "actions": ["../venv/bin/pip-sync ../requirements.dev.txt"],
    }


# def task_gray_format():
#     """gray code format for service
#     https://github.com/dizballanze/gray"""
#     return {
#         "actions": [
#             "../venv/bin/gray ../**/*.py --isort-virtual-env ../venv --min-python-version 3.8",
#             "../venv/bin/gray ../*.py --isort-virtual-env ../venv --min-python-version 3.8",
#         ],
#     }

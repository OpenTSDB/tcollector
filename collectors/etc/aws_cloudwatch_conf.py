#!/usr/bin/env python
import os

aws_profile_default = 'cloudwatch'
aws_access_key_default = '<access_key_id>'
aws_secret_access_key_default = '<secret_access_key>'

def enabled():
    return True


# Some corner cases such as "export VAR=" and similar may result in a var being defined but not valid
def get_validate_env_var(var, default):
    value = os.getenv(var, default)
    if value is None or value == "":
        return default
    else:
        return value


def get_accesskey_secretkey():
    access_key_id = get_validate_env_var('AWS_ACCESS_KEY_ID', aws_access_key_default)
    secret_access_key = get_validate_env_var('AWS_SECRET_ACCESS_KEY', aws_secret_access_key_default)
    return (access_key_id, secret_access_key)


def get_aws_profile():
    return get_validate_env_var('AWS_PROFILE', aws_profile_default)

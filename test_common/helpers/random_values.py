"""Random-value generators for PostgreSQL (and Spark) test data."""

import datetime
import json
import math
import os
import random
import re
import string
import tempfile
import uuid
import zoneinfo

import psycopg2


def generate_random_file_path():
    temp_dir = tempfile.gettempdir()  # Get the system's temporary directory
    random_file_name = str(uuid.uuid4())  # Generate a unique file name
    return os.path.join(temp_dir, random_file_name)


def generate_random_numeric_typename(max_precision=38):
    precision = random.randint(1, max_precision)
    scale = random.randint(0, precision - 1)
    return f"numeric({precision},{scale})"


def generate_random_value(pg_type_name):
    if pg_type_name == "smallint" or pg_type_name == "int2":
        pg_val = random.randint(-32768, 32767)
        spark_val = pg_val
    elif pg_type_name == "int" or pg_type_name == "int4":
        pg_val = random.randint(-2147483648, 2147483647)
        spark_val = pg_val
    elif pg_type_name == "bigint" or pg_type_name == "int8":
        pg_val = random.randint(-9223372036854775808, 9223372036854775807)
        spark_val = pg_val
    elif pg_type_name == "float4":
        pg_val = f"{random.uniform(-(10**4), 10**4):e}"
        spark_val = pg_val
    elif pg_type_name == "float8":
        pg_val = f"{random.uniform(-(10**4), 10**4):e}"
        spark_val = pg_val
    elif pg_type_name.startswith("numeric"):
        match = re.match(r"numeric\((\d+),-?(\d+)\)", pg_type_name)
        precision = int(match.group(1)) if match is not None else 38
        scale = int(match.group(2)) if match is not None else 9
        to_power = precision - scale

        def truncate_float(x):
            if x == 0:
                return 0
            else:
                factor = 10.0**scale
                return math.trunc(x * factor) / factor

        pg_val = truncate_float(
            random.uniform(-(10**to_power) + 1e-6, 10**to_power - 1e-6)
        )
        spark_val = f"{pg_val}BD"
    elif pg_type_name.startswith("varchar"):
        match = re.match(r"varchar\((\d+)\)", pg_type_name)
        length = int(match.group(1)) if match is not None else 20
        val = "".join(random.choices(string.ascii_lowercase, k=length))
        pg_val = f"'{val}'"
        spark_val = pg_val
    elif pg_type_name.startswith("bpchar"):
        match = re.match(r"bpchar\((\d+)\)", pg_type_name)
        length = int(match.group(1)) if match is not None else 20
        val = "".join(random.choices(string.ascii_lowercase, k=length))
        pg_val = f"'{val}'"
        spark_val = pg_val
    elif pg_type_name == "text":
        val = "".join(random.choices(string.ascii_lowercase, k=10))
        pg_val = f"'{val}'"
        spark_val = pg_val
    elif pg_type_name == '"char"':
        val = "".join(random.choices(string.ascii_lowercase, k=1))
        pg_val = f"'{val}'"
        spark_val = pg_val
    elif pg_type_name.startswith("char"):
        match = re.match(r"char\((\d+)\)", pg_type_name)
        length = int(match.group(1)) if match is not None else 1
        val = "".join(random.choices(string.ascii_lowercase, k=length))
        pg_val = f"'{val}'"
        spark_val = pg_val
    elif pg_type_name == "bytea":
        val = bytes(random.choices(range(256), k=10))
        pg_val = psycopg2.Binary(val)
        spark_val = f"X'{val.hex()}'"
    elif pg_type_name == "date":
        month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        year = random.randint(1, 9999)
        month = random.randint(1, 12)
        day = random.randint(1, month_days[month - 1])
        val = datetime.date(year, month, day)
        pg_val = psycopg2.Date(year, month, day)
        spark_val = f"date '{str(val)}'"
    elif pg_type_name == "time":
        pg_val = psycopg2.Time(
            random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
        )
        spark_val = None  # not applicable type for spark
    elif pg_type_name == "timetz":
        pg_val = psycopg2.Time(
            random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
        )
        spark_val = None  # not applicable type for spark
    elif pg_type_name == "timestamp":
        month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        year = random.randint(1, 9999)
        month = random.randint(1, 12)
        day = random.randint(1, month_days[month - 1])
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        val = datetime.datetime(year, month, day, hour, minute, second)
        pg_val = psycopg2.Timestamp(year, month, day, hour, minute, second)
        spark_val = f"timestamp_ntz '{str(val)}'"
    elif pg_type_name == "timestamptz":
        month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        # bug for years <= 1910 (Old repo: issues/1279)
        year = random.randint(1911, 9998)
        month = random.randint(1, 12)
        day = random.randint(1, month_days[month - 1])
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        tzinfo_utc = zoneinfo.ZoneInfo("UTC")
        val = datetime.datetime(
            year, month, day, hour, minute, second, tzinfo=tzinfo_utc
        )
        pg_val = psycopg2.Timestamp(year, month, day, hour, minute, second, tzinfo_utc)
        spark_val = f"timestamp '{str(val)}'"
    elif pg_type_name == "boolean" or pg_type_name == "bool":
        pg_val = random.choice([True, False])
        spark_val = pg_val
    elif pg_type_name == "interval":
        years = random.randint(2, 1000)
        months = random.randint(2, 100)
        days = random.randint(2, 1000)
        hours = random.randint(2, 100)
        minutes = random.randint(2, 100)
        seconds = random.randint(2, 100)
        pg_val = f"'{years} years {months} months {days} days {hours} hours {minutes} minutes {seconds} seconds'"
        spark_val = None  # not applicable type for spark
    elif pg_type_name == "uuid":
        pg_val = f"'{uuid.uuid4()}'"
        spark_val = None  # not applicable type for spark
    elif pg_type_name == "json" or pg_type_name == "jsonb":
        generate_random_key = lambda: generate_random_value("varchar(10)").strip("'")
        generate_random_str_value = lambda: generate_random_value("varchar(10)").strip(
            "'"
        )
        generate_random_int_value = lambda: generate_random_value("int")
        generate_random_float_value = lambda: generate_random_value("float8")

        num_keys = random.randint(1, 5)
        json_obj = {}
        for _ in range(num_keys):
            key, _ = generate_random_key()
            val, _ = random.choice(
                [
                    generate_random_str_value,
                    generate_random_int_value,
                    generate_random_float_value,
                ]
            )()
            json_obj[key] = val

        pg_val = f"'{json.dumps(json_obj)}'"
        spark_val = None  # not applicable type for spark
    else:
        raise ValueError(f"Unsupported pg type: {pg_type_name}")

    return (str(pg_val), str(spark_val))

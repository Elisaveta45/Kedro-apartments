from datetime import datetime

import pandas as pd
from typing import Any, Callable, Dict, Set, Optional

DATE_FORMAT = r"%Y%m%dT%H%M%S"
FD = None
TD = None
MONTH_NAMES = [
    "януари",
    "февруари",
    "март",
    "април",
    "май",
    "юни",
    "юли",
    "август",
    "септември",
    "октомври",
    "ноември",
    "декември",
]
MONTH_NAME_TO_IDX = {
    month_name: (idx + 1) for idx, month_name in enumerate(MONTH_NAMES)
}
MONTH_NAME_TO_STR_IDX = {
    month_name: str(idx + 1) for idx, month_name in enumerate(MONTH_NAMES)
}

KNOWN_APARTMENT_FIELDS: Set[str] = {
    "Вода:",
    "Газ:",
    "Двор:",
    "Етаж:",
    "Категория:",
    "Квадратура:",
    "Начин на ползване:",
    "Площ:",
    "Регулация:",
    "Строителство:",
    "ТEЦ:",
    "Ток:",
}

APARTMENT_FIELD_TO_DICT_KEY = {
    "Вода:": "water",
    "Газ:": "gas",
    "Двор:": "garden",
    "Етаж:": "floor",
    "Категория:": "category",
    "Квадратура:": "sqm",
    "Начин на ползване:": "usage",
    "Площ:": "space",
    "Регулация:": "regulations",
    "Строителство:": "built",
    "ТEЦ:": "tec",
    "Ток:": "electricity",
}


def concat_partitions(
    partitioned_input: Dict[str, Callable[[], Any]],
    from_date: Optional[datetime],
    to_date: Optional[datetime],
) -> pd.DataFrame:
    """Concatenate input partitions into one pandas DataFrame.

    Args:
        partitioned_input: A dictionary with partition ids as keys and load functions as values.
        from_date:
        to_date:

    Returns:
        Pandas DataFrame representing a concatenation of all loaded partitions.
    """
    result = pd.DataFrame()

    for partition_key, partition_load_func in sorted(
        partitioned_input.items()
    ):
        partition_folder = partition_key.split("/")[0]
        partition_datetime = pd.to_datetime(
            partition_folder, format="%Y%m%dT%H%M%S"
        )
        if from_date is not None and partition_datetime < from_date:
            continue
        if to_date is not None and partition_datetime > to_date:
            continue

        partition_data = (
            partition_load_func()
        )  # load the actual partition data
        # concat with existing result
        result = pd.concat(
            [result, partition_data], ignore_index=True, sort=True
        )

    result.reset_index(drop=True, inplace=True)
    result.index.name = "id"
    return result


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    df.drop_duplicates(subset=["link", "entry_time"], inplace=True)

    return df


def merge_insert_update_time(df: pd.DataFrame) -> pd.DataFrame:
    df["entry_time"] = df["last_updated"].combine_first(df["created_at"])
    df.drop(["created_at", "last_updated"], axis=1, inplace=True)
    cond = df.entry_time.str.contains("Коригирана", na=False)

    df.loc[cond, "is_creation"] = True
    df.loc[~cond, "is_creation"] = False

    return df


def floor_to_sqm(df: pd.DataFrame) -> pd.DataFrame:
    cond = df.floor.str.contains(r"\d+ кв.м", na=False)

    df.loc[cond, "sqm"] = df.loc[cond, "floor"]
    df.loc[cond, "floor"] = None

    return df


def floor_to_built(df: pd.DataFrame) -> pd.DataFrame:
    cond = df.floor.str.contains(r".* \d+ г.", na=False)

    df.loc[cond, "built"] = df.loc[cond, "floor"]
    df.loc[cond, "floor"] = None

    return df


def swap_space_sqm(df: pd.DataFrame) -> pd.DataFrame:
    cond = (df["space"].notna()) & (df["sqm"].isnull())
    df.loc[cond, ["space", "sqm"]] = df.loc[cond, ["sqm", "space"]].values
    return df


def extract_floor(df: pd.DataFrame) -> pd.DataFrame:
    new = df.floor.str.extract(
        r"(?P<new_floor>\d+|Партер)((?P<help>\D+)(?P<max_floor>\d+))?"
    )

    new['new_floor'] = new['new_floor'].replace("Партер", 0)
    df.loc[:, ["floor", "max_floor"]] = new[["new_floor", "max_floor"]]

    return df


def extract_area(df: pd.DataFrame) -> pd.DataFrame:
    new = df.sqm.str.split(" ", n=1, expand=True)
    df["apartment_area"], df["area_type"] = new[0], new[1]
    df["apartment_area"] = df["apartment_area"].astype(float)
    df.drop(["sqm"], axis=1, inplace=True)

    return df


def extract_price(df: pd.DataFrame) -> pd.DataFrame:
    new = df.price.str.rsplit(" ", n=1, expand=True)
    new[0] = new[0].str.replace(" ", "")
    df["price"], df["price_currency"] = new[0], new[1]
    df["price"] = df["price"].astype(float)

    return df

import re
import math
import pytz
import logging
import datetime
import numpy as np
import pandas as pd
import unicodedata
from typing import List, Tuple, Union

from sklearn.utils import compute_class_weight

from dbbinance.fetcher.constants import Constants

__version__ = 0.0022

logger = logging.getLogger()

timedelta_dict: dict = {'d': 'days',
                        's': 'seconds',
                        'm': 'minutes',
                        'h': 'hours',
                        'w': 'weeks',
                        'M': 'months'
                        }


def check_convert_to_datetime(check_datetime: datetime.datetime or str or int or None, utc_aware=True):
    if isinstance(check_datetime, datetime.datetime):
        checked = check_datetime
    elif isinstance(check_datetime, str):
        checked = datetime.datetime.strptime(check_datetime, Constants.default_datetime_format)
    elif isinstance(check_datetime, int):
        checked = datetime.datetime.fromtimestamp(check_datetime / 1e3)
    elif check_datetime is None:
        checked = None
    else:
        assert isinstance(check_datetime, (datetime.datetime or str or int or None)), \
            f'Error: {check_datetime} type is unknown {type(check_datetime)}'
        checked = None
    if utc_aware and checked is not None:
        if utc_aware:
            checked = checked.replace(tzinfo=pytz.utc)
    return checked


def convert_timeframe_to_freq(timeframe) -> str:
    """
    Convert timeframe for resample function
    Args:
        timeframe (str):    timeframe

    Returns:
        timeframe (str):    converted timeframe
    """
    if timeframe[-1] == 'm':
        frequency = f'{timeframe[:-1]}min'
    elif timeframe[-1] == 'h':
        frequency = f'{timeframe[:-1]}H'
    else:
        frequency = timeframe.upper()
    return frequency


def get_timeframe_bins(current_timeframe: str = '1h'):
    timeframe_freq = current_timeframe[-1:]
    timeframe_qty = int(current_timeframe[:-1])
    timeframe_binsize = Constants.binsizes.get(current_timeframe, None)
    """ If we have non-standard timeframe """
    if timeframe_binsize is None:
        timeframe_binsize = Constants.binsizes.get(f'1{timeframe_freq}', None)
        timeframe_binsize *= timeframe_qty
    return timeframe_binsize


def get_nearest_timeframe(bins: int) -> str:
    previous_timeframe_name = '1m'
    result = previous_timeframe_name
    for timeframe_name, timeframe_bins in Constants.binsizes.items():
        if timeframe_bins == bins:
            result = timeframe_name
            break
        elif timeframe_bins > bins:
            _temp = get_timeframe_bins(previous_timeframe_name)
            previous_period = int(previous_timeframe_name[:-1])
            if previous_period > 1:
                _temp = Constants.binsizes.get(f'1{previous_timeframe_name[-1]}')
            for period in range(previous_period, 500):
                if period * _temp > bins:
                    break
                else:
                    previous_period = period
            result = f'{previous_period + 1}{previous_timeframe_name[-1]}'
            break
        else:
            previous_timeframe_name = f'{timeframe_name}'
    return result

def get_timedelta_kwargs(_period: int or str, current_timeframe: str = "1h") -> dict:
    """

    Args:
        _period (int or str):
        current_timeframe (str):

    Returns:
        timedelta_kwargs (dict):    Example: ("hours": 10)
    """
    # timeframe_freq = current_timeframe[-1:]
    # timeframe_qty = int(current_timeframe[:-1])
    # timeframe_binsize = Constants.binsizes.get(current_timeframe, None)
    # """ If we have non-standard timeframe """
    # if timeframe_binsize is None:
    #     timeframe_binsize = Constants.binsizes.get(f'1{timeframe_freq}', None)
    #     timeframe_binsize *= timeframe_qty
    timeframe_freq = current_timeframe[-1:]
    timeframe_binsize = get_timeframe_bins(current_timeframe)

    if isinstance(_period, int):
        _period_bins = int(_period)
        freq = timedelta_dict.get(timeframe_freq, None)
        freq_qty_in_minutes = _period_bins * timeframe_binsize
        freq_qty = freq_qty_in_minutes // Constants.binsizes.get(f'1{timeframe_freq}', None)
    elif isinstance(_period, str):
        try:
            _period_qty = int(_period[:-1])
            _period_freq = _period[-1:]
            assert _period_freq in timedelta_dict.keys(), f'Error: unknown frequency {_period_freq}. ' \
                                                          f'Valid frequency {timedelta_dict.keys()}'
        except ValueError:
            assert _period[:-1].isdigit, f'Error: syntax of walk_period={_period} is not correct'

        freq = timedelta_dict.get(_period_freq, None)
        freq_qty = _period_qty

    timedelta_kwargs: dict = {f'{freq}': freq_qty}
    return timedelta_kwargs


def get_freq_starts(temp_df: pd.DataFrame, freq):
    freq_starts = pd.date_range(temp_df.index[0], temp_df.index[1], freq=freq)
    return freq_starts


def minmax_normalization_1_1(new_data):
    new_data = (new_data - new_data.min()) / (new_data.max() - new_data.min()) * 2 + -1
    return new_data


def minmax_normalization_custom(new_data, features_range=(-1, 1)):
    new_data = (new_data - new_data.min()) / (new_data.max() - new_data.min()) * (
            features_range[1] - features_range[0]) + features_range[0]
    return new_data


def minmax_normalization(new_data):
    new_data = (new_data - new_data.min()) / (new_data.max() - new_data.min())
    return new_data


def std_normalization(new_data):
    new_data = (new_data - new_data.mean()) / new_data.std()
    return new_data


def mean_normalization(new_data):
    new_data = (new_data - new_data.mean()) / new_data.mean()
    return new_data


def logarithmicscaler(new_data):
    z = np.log10(new_data + 1)
    _temp = 1 / (1 + (math.e ** -z))  # original version
    return _temp


def set_apply_func(apply_func):
    if apply_func is None or apply_func == 'minmaxscaler':
        func = minmax_normalization
    elif apply_func == 'minmaxscaler_1_1':
        func = minmax_normalization_1_1
    elif apply_func == 'logarithmicscaler':
        func = logarithmicscaler
    elif apply_func == 'meannormalizer':
        func = mean_normalization
    elif apply_func == 'standardnormalizer':
        func = std_normalization
    assert (apply_func == "minmaxscaler"
            or apply_func == "minmaxscaler_1_1"
            or apply_func == "logarithmicscaler"
            or apply_func == "meannormalizer"
            or apply_func == "standardnormalizer"
            or apply_func is None
            ), \
        f'Unknown rolling scaler {apply_func}'
    return func


def is_ones_zeros(horizon_targets) -> bool:
    """
    Checking horizon_targets for OHE

    Returns:
        status (bool):  checking
                        if all channels OHE - returns True
                        if not - returns False
    """
    present_data = np.unique(horizon_targets)
    return np.array_equal(present_data, [0, 1])


def is_categorical(horizon_targets_channel: Union[list, np.ndarray],
                   return_count: bool = False,
                   max_unique: int = 101,
                   ) -> Union[Tuple[bool, int], bool]:
    """
    Check if data categorical (all integers with less than 101 unique)

    Args:
        horizon_targets_channel (Union[list, np.ndarray]):  ONE channel data with shape (x, )
        return_count (bool):                                return unique counts not
        max_unique (int):                                   max unique to decide it's categorical

    Returns:
        Union[Tuple[bool, int], bool]:                      True if categorical and False if not
    """
    unq = np.unique(horizon_targets_channel)
    unq_count = len(unq)
    is_str = np.vectorize(lambda x: isinstance(x, str))

    if unq_count > max_unique or np.any(is_str(unq)):
        result = (False, unq_count)
    elif np.all(np.round(unq, decimals=1) == unq):
        result = (True, unq_count)
    else:
        result = (False, unq_count)

    return result if return_count else result[0]


def is_ohe_subset(_data):
    if _data.ndim == 1:
        return False
    sum_cols = np.sum(_data, axis=1)
    # If the sum is always 1, add this subset to valid_ohe_dict
    if np.min(sum_cols) == 1 and np.max(sum_cols) == 1 and is_ones_zeros(_data):
        return True
    else:
        return False


def check_y_cat_or_regression(_data) -> tuple:
    """ check for categorical or regression data """
    cat_or_not, unq_count = is_categorical(_data.squeeze(), return_count=True)
    if cat_or_not:
        targets_type = "categorical"
        targets_cats: List[tuple] = [("categorical", (0, 1)), ]
        loss = "sparse_categorical_crossentropy"
    else:
        targets_type = "regression"
        targets_cats: List[tuple] = [("regression", (0, 1)), ]
        loss = "mse"
    return targets_type, targets_cats, loss


def check_y_type(_data) -> tuple:
    """ check for categorical or regression data """
    if is_ones_zeros(_data):
        targets_type = "ohe"
        targets_cats: List[tuple] = [("ohe", (0, _data.shape[1])), ]
        return targets_type, targets_cats
    else:
        return check_y_cat_or_regression(_data)


def check_data_horizons(horizons_data_list) -> tuple:
    data_shapes = list()

    """ Collect shapes from all data horizons """
    for horizon_data in horizons_data_list:
        data_shapes.append(horizon_data.shape)

    """ Check what all data shapes is the same """
    data_shapes = list(set(data_shapes))

    assert len(data_shapes) == 1, (
        f"Error: horizons data must have same shape but now horizon data with idx {np.argmax(list(data_shapes))} is different")

    return data_shapes[0]


def check_targets_horizons(horizons_targets_list) -> tuple:
    trgts_shapes = list()

    """ Collect shapes from all targets horizons """
    for horizon_targets in horizons_targets_list:
        trgts_shapes.append(horizon_targets.shape)

    """ Check what all targets shapes is the same """
    trgts_shapes = list(set(trgts_shapes))

    assert len(trgts_shapes) == 1, (f"Error: horizons targets must have same shape but "
                                    f"now horizon targets with idx {np.argmax(list(trgts_shapes))} is different")

    return trgts_shapes[0]


def check_horizons_shapes(horizons_data_list, horizons_targets_list) -> tuple:
    """ Check qty of horizons data and targets """
    assert len(horizons_data_list) == len(horizons_targets_list), (f"Error: q-ty of horizons data and horizons targets "
                                                                   f"is not equal - qty data {len(horizons_data_list)} "
                                                                   f"!= qty targets {len(horizons_targets_list)}")

    data_shape = check_data_horizons(horizons_data_list)
    trgts_shape = check_targets_horizons(horizons_targets_list)

    assert data_shape[0] == trgts_shape[0], (f"Error: 1st dimension of data and targets must have "
                                             f"same shape but now it's different - "
                                             f"{data_shape[0]} != {trgts_shape[0]}")

    return data_shape, trgts_shape


def convert_horizons_df_to_values(x_list: List[pd.DataFrame]) -> List[np.ndarray]:
    _temp_data_list = list()
    for _x in x_list:
        _temp_data_list.append(_x.values)
    return _temp_data_list


def calc_classes_weights(y_data):
    classes, classes_counts = np.unique(y_data.copy(), return_counts=True)
    if not len(classes) > 1:
        logger.warning(
            f"{__name__}: Y data have only ONE label. Check the Y data, "
            f"or make this part longer for unbalanced datasets. Using dummy numbers for 2 classes ")
        """ Using dummy numbers """
        if classes[0] == 0:
            classes = [0, 1]
            sample_weight = [0.99, 0.1]
        else:
            classes = [0, 1]
            sample_weight = [0.1, 0.99]
        classes_weights = dict(zip(classes, sample_weight))
    else:
        sample_weight = compute_class_weight(class_weight='balanced', classes=classes, y=y_data)
        classes_weights = dict(zip(classes, sample_weight))
    return list(classes), list(classes_counts), classes_weights


def calc_batches(data_length, batch_size, stride) -> int:
    batches = int(data_length // (batch_size * stride))
    batches = batches if data_length % (batch_size * stride) == 0 else batches + 1
    return batches


def calc_sample_weight(classes_weights: dict, y_onehot_data: np.ndarray):
    new_y_onehot_data = y_onehot_data.copy()
    for ix, class_weight in classes_weights.items():
        new_y_onehot_data[:, ix] = y_onehot_data[:, ix] * class_weight
    return new_y_onehot_data


def slugify(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')
